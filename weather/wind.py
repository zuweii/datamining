# wind.py
import xarray as xr

fn = "wind.f000"

# Option 1: open both 10 m winds in one go (works on most GFS files)
ds = xr.open_dataset(
    fn, engine="cfgrib",
    backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": 10, "stepType": "instant"}}
)
print(list(ds.data_vars))  # you'll see names like UGRD_10maboveground, VGRD_10maboveground

# tidy names
rename = {}
for k in ds.data_vars:
    if "UGRD" in k or k.lower().startswith(("10u","u10")): rename[k] = "u10"
    if "VGRD" in k or k.lower().startswith(("10v","v10")): rename[k] = "v10"
ds = ds.rename(rename)
ds.to_netcdf("wind10m.nc")
print("Saved wind10m.nc with u10, v10")
