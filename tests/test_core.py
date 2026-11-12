import random
from datetime import date

from src.common.emissions import carbon_intensity, clean_generation
from src.common.streaming_algos import BloomFilter, DGIM, RecordClassifier, ReservoirSample
from src.common.timeutil import iso_utc, pacific_to_utc
from src.producers.caiso_producer import _parse_times


def test_ci_basic():
    r = carbon_intensity({"Solar": 600, "Natural Gas": 300, "Imports": 100})
    assert abs(r["ci"] - (300 * 411 + 100 * 428) / 1000) < 1e-9
    assert abs(r["renewable_share"] - 0.6) < 1e-9


def test_battery_charging_removed_from_denominator():
    with_charge = carbon_intensity({"Solar": 800, "Natural Gas": 200, "Batteries": -300})
    without = carbon_intensity({"Solar": 800, "Natural Gas": 200})
    assert with_charge == without and clean_generation({"Batteries": -5})["Batteries"] == 0.0


def test_ci_empty():
    assert carbon_intensity({"Batteries": -10})["ci"] is None


def test_dst_spring_forward_and_fall_back():
    assert iso_utc(pacific_to_utc("2026-07-01", "12:00")) == "2026-07-01T19:00:00Z"      # PDT = UTC-7
    assert iso_utc(pacific_to_utc("2026-01-15", "12:00")) == "2026-01-15T20:00:00Z"      # PST = UTC-8
    rows = [{"Time": t} for t in ["00:55", "01:00", "01:55", "01:00", "01:55", "02:00"]]   # 2026-11-01 repeats 01:xx
    utc = [iso_utc(u) for u, _ in _parse_times("2026-11-01", rows)]
    assert len(set(utc)) == len(utc) and utc == sorted(utc)           # second pass maps to the later UTC hour


def test_bloom_no_false_negatives_low_fp():
    bf = BloomFilter(capacity=5000, fp_rate=0.01)
    for i in range(5000):
        bf.add(f"k{i}")
    assert all(f"k{i}" in bf for i in range(5000))
    fp = sum(f"x{i}" in bf for i in range(5000)) / 5000
    assert fp < 0.03


def test_record_classifier():
    c = RecordClassifier(capacity=1000)
    assert c.classify("t1", "a") == "new"
    assert c.classify("t1", "a") == "duplicate"
    assert c.classify("t1", "b") == "revised"
    assert c.classify("t1", "b") == "duplicate"


def test_dgim_within_bound():
    rng, d, bits = random.Random(1), DGIM(288), []
    worst = 0
    for i in range(5000):
        b = 1 if rng.random() < 0.4 else 0
        bits.append(b); d.update(b)
        if i > 288:
            exact = sum(bits[-288:])
            worst = max(worst, abs(d.count() - exact) / max(exact, 1))
    assert worst <= 0.5


def test_reservoir_uniform():
    counts = [0] * 10
    for seed in range(300):
        rs = ReservoirSample(k=3, seed=seed)
        for x in range(10):
            rs.add(x)
        for x in rs.items:
            counts[x] += 1
    assert min(counts) > 0.6 * (300 * 3 / 10) and max(counts) < 1.4 * (300 * 3 / 10)


def test_caiso_csv_parsing(monkeypatch):
    from src.producers import caiso_producer as cp

    class R:  # minimal Response stand-in
        text = "Time,Solar,Natural Gas,Batteries,Imports\n00:00,0,8000,2000,6000\n00:05,0,8100,-100,6000\n"

    monkeypatch.setattr(cp, "http_get", lambda s, u, **k: R())
    out = cp.fetch_fuel_day(None, date(2026, 6, 1))
    assert out[0]["interval_utc"] == "2026-06-01T07:00:00Z"                  # 00:00 PDT
    assert out[1]["fuels"]["Batteries"] == -100.0


def test_fuel_names_canonical_and_negative_solar(monkeypatch):
    from src.producers import caiso_producer as cp

    class R:
        text = "Time,Solar,Large Hydro,Natural Gas\n00:00,-64,2389,9701\n"

    monkeypatch.setattr(cp, "http_get", lambda s, u, **k: R())
    f = cp.fetch_fuel_day(None, date(2026, 9, 1))[0]["fuels"]
    assert "Large hydro" in f and f["Solar"] == -64.0
    assert abs(carbon_intensity(f)["renewable_share"] - 2389 / (2389 + 9701)) < 1e-12
