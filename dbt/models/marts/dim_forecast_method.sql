select * from (values
  ('persistence', 'Tomorrow equals today at the same hour'),
  ('net_load', 'Nearest historical hours by forecast net load'),
  ('lsh_analog', 'LSH-retrieved analog days')) as t(method, description)
