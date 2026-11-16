# EV savings: sensitivity to parking-window length

Author: Karishma Routhu (Part C, forecasting and scheduling)

## What I changed

The EV savings study (`eval/savings_study.py`) used one fixed set of simulated parking windows, so the report said
"We did not sweep the window length." I refactored the study into a function, `run_study()`, that takes a cap on the
parking window. I then repeated the study for caps of 3, 6 and 12 hours and print one saving per cap.

All three runs use the **same simulated sessions** (22,286 sessions, 314,147 kWh), so only the window length changes.
Each run works on a copy of the sessions: it limits each car's departure to at most `arrive_hour + cap`, then compares
charging on arrival (baseline) with charging in the cleanest hours inside the window (shifted).

## Results

Run on the real hourly carbon-intensity history, read through the read-only Snowflake login.

| Max window | Usable sessions | Baseline (kg CO2) | Shifted (kg CO2) | Saved (kg CO2) | Saved (%) |
|---|---|---|---|---|---|
| 3 h  | 19,948 of 22,286 | 29,108.9 | 26,357.1 |  2,751.8 | 9.5%  |
| 6 h  | 22,225 of 22,286 | 34,826.5 | 24,967.8 |  9,858.7 | 28.3% |
| 12 h | 22,225 of 22,286 | 34,826.5 | 24,399.2 | 10,427.3 | 29.9% |

Check: the 12 h line equals the main result of the same run (34,826.5 kg to 24,399.2 kg, 29.9%), because the
simulated parking windows already cap at 12 hours.

## What it means

- The saving grows with the window, because a longer window gives the scheduler more hours to choose from.
- The biggest step is from 3 h to 6 h (9.5% to 28.3%). From 6 h to 12 h the gain is small (28.3% to 29.9%).
- With a 6 hour window or longer, almost the whole saving is already reached. With only 3 hours, little is gained.

## Caveats

- **Usable sessions.** A session is left out when its window has fewer hours than it needs to charge (the simulation
  returns zero for it). At 3 h this drops 2,338 sessions; at 6 h and 12 h it drops 61. The percentage is calculated
  only on the usable sessions.
- **The 3 h figure is not like-for-like with the others.** It is measured on a smaller set of sessions (and has a lower
  baseline because of that), so compare it with the other rows with care. The baseline is the same at 6 h and 12 h
  because the same sessions are usable and the baseline charges in the first hours after arrival.
- **The sessions are simulated.** No real charger data is available. The carbon intensity is real.
- **Average, not marginal, intensity.** The study uses the grid's average carbon intensity, not the marginal intensity of
  the next unit of power.
- **Small difference from the report.** The report states 30.0% saved (34,791 kg to 24,363 kg) for the same 22,286
  sessions and 314,147 kWh. My run gives 29.9% (34,826.5 kg to 24,399.2 kg). I reported this difference to Shashidhar.

## How to reproduce

```
python -m eval.savings_study
```

Needs the read-only Snowflake settings in `.env` and `SNOWFLAKE_READONLY=1`. The sweep is printed under
"Window sweep".

## AI use

I used Claude (Anthropic) to help write the sweep code and this note. I reviewed the code and ran it myself, and the
numbers above are copied from my own run.
