import math

import unittest

from osintxpress import compute_orbits


# Known valid TLE for the International Space Station (ISS)
ISS_LINE1 = "1 25544U 98067A   20194.88612269 -.00002218  00000-0 -31515-4 0  9992"
ISS_LINE2 = "2 25544  51.6461 221.2784 0001413  89.1723 280.4612 15.49507896236008"

# Known valid TLE for a generic Low Earth Orbit (LEO) satellite
LEO_LINE1 = "1 42982U 98067NE  20194.06866787  .00008489  00000-0  72204-4 0  9997"
LEO_LINE2 = "2 42982  51.6338 155.6245 0002758 166.8841 193.2228 15.70564504154944"


class TestSatelliteLogic(unittest.TestCase):

    def test_valid_tle_propagation(self):
        """Test that a valid TLE returns sane geodetic coordinates."""
        
        lats, lons, alts = compute_orbits([ISS_LINE1], [ISS_LINE2])
        
        self.assertEqual(len(lats), 1)
        self.assertEqual(len(lons), 1)
        self.assertEqual(len(alts), 1)
        
        # Note: Because the Rust function uses Utc::now() internally, 
        # the exact coordinates change every second. We test the bounding boxes instead.
        self.assertTrue(-90.0 <= lats[0] <= 90.0, f"Latitude {lats[0]} out of bounds")
        self.assertTrue(-180.0 <= lons[0] <= 180.0, f"Longitude {lons[0]} out of bounds")
        self.assertTrue(alts[0] > 200.0, f"Altitude {alts[0]}km is too low for orbit")


    def test_invalid_tle_graceful_degradation(self):
        """Test that garbage TLE strings do not panic Rust, but return NaNs."""

        garbage_l1 = "1 00000U GARBAGE DATA THAT SGP4 SHOULD REJECT"
        garbage_l2 = "2 00000 MORE GARBAGE DATA"
        
        lats, lons, alts = compute_orbits([garbage_l1], [garbage_l2])
        
        self.assertEqual(len(lats), 1)
        self.assertTrue(math.isnan(lats[0]), "Expected NaN latitude for garbage TLE")
        self.assertTrue(math.isnan(lons[0]), "Expected NaN longitude for garbage TLE")
        self.assertTrue(math.isnan(alts[0]), "Expected NaN altitude for garbage TLE")


    def test_batch_processing(self):
        """Test that PyO3 can handle multiple satellites simultaneously."""

        l1s = [ISS_LINE1, LEO_LINE1]
        l2s = [ISS_LINE2, LEO_LINE2]
        
        lats, lons, alts = compute_orbits(l1s, l2s)
        
        self.assertEqual(len(lats), 2)
        # Verify both are valid floats
        self.assertFalse(math.isnan(lats[0]))
        self.assertFalse(math.isnan(lats[1]))


    def test_mixed_valid_and_invalid_batch(self):
        """Test that a batch with mixed good and bad data processes correctly."""

        l1s = [ISS_LINE1, "GARBAGE", LEO_LINE1]
        l2s = [ISS_LINE2, "GARBAGE", LEO_LINE2]
        
        lats, lons, alts = compute_orbits(l1s, l2s)
        
        self.assertEqual(len(lats), 3)
        self.assertFalse(math.isnan(lats[0])) # ISS is good
        self.assertTrue(math.isnan(lats[1]))  # Garbage is NaN
        self.assertFalse(math.isnan(lats[2])) # LEO is good


    def test_empty_lists(self):
        """Test that passing empty lists doesn't panic."""

        lats, lons, alts = compute_orbits([], [])
        
        self.assertEqual(len(lats), 0)
        self.assertEqual(len(lons), 0)
        self.assertEqual(len(alts), 0)


if __name__ == '__main__':

    unittest.main(verbosity=2)