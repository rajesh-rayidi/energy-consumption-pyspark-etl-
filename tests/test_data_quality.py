def test_total_readings():
  assert 2049040 + 25979 == 2075019
def test_missing_reading_percentage():
  assert 1.2 < (25979 / 2075019) * 100 < 1.3
