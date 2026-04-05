use chrono::NaiveDateTime;
use sgp4::{Constants, Elements};
use std::f64::consts::PI;

pub struct SatellitePosition {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude_km: f64,
}

pub fn compute_satellite_position(
    name: &str,
    line1: &str,
    line2: &str,
    now: &NaiveDateTime,
) -> Result<SatellitePosition, String> {
    
    // Parse TLE into SGP4 Elements
    let elements = Elements::from_tle(Some(name.to_owned()), line1.as_bytes(), line2.as_bytes())
        .map_err(|e| format!("TLE Parse Error: {}", e))?;
        
    // Initialize the propagator
    let constants = Constants::from_elements(&elements)
        .map_err(|e| format!("Propagator Error: {}", e))?;
        
    // Calculate minutes since the TLE epoch
    let mins_since_epoch = elements.datetime_to_minutes_since_epoch(now)
        .map_err(|e| format!("Time Error: {}", e))?;
        
    // Propagate to get Cartesian (X,Y,Z) velocity vectors in space
    let prediction = constants.propagate(mins_since_epoch)
        .map_err(|e| format!("Math Error: {}", e))?;

    let x = prediction.position[0];
    let y = prediction.position[1];
    let z = prediction.position[2];

    // Convert Cartesian (X,Y,Z) to Geodetic (Lat, Lon, Alt)
    // Using a fast spherical Earth approximation (Radius ~6371km)
    let r = (x * x + y * y + z * z).sqrt();
    let altitude_km = r - 6371.0; 

    let lat = (z / r).asin() * (180.0 / PI);
    let lon = y.atan2(x) * (180.0 / PI);

    // Adjust longitude for Earth's rotation (Greenwich Mean Sidereal Time)
    let j2000_days = sgp4::julian_years_since_j2000(now) * 365.25;
    let earth_rotation_deg = (j2000_days * 360.985647366) % 360.0;
    
    let mut final_lon = lon - earth_rotation_deg;
    
    // Wrap longitude to standard [-180, 180] map coordinates
    while final_lon < -180.0 { final_lon += 360.0; }
    while final_lon > 180.0 { final_lon -= 360.0; }

    Ok(SatellitePosition {
        latitude: lat,
        longitude: final_lon,
        altitude_km,
    })
}

