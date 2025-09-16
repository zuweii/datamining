# join_all.py
# Usage:
#   python join_all.py ais.csv waves.nc currents.nc wind10m.nc out.csv
# (currents or wind can be "-" if you don't have them yet)

import sys, numpy as np, pandas as pd, xarray as xr
from pathlib import Path

def pick(ds, candidates):
    for c in candidates:
        if c in ds.coords or c in ds.dims:
            return c
    raise KeyError(f"None of {candidates} in coords/dims {list(ds.coords)}")

def sample_vars_to_points(nc_path, ais_df, wanted):
    """
    Samples variables from an xarray dataset at AIS (time, lat, lon) points.
    - Works with 1-D or 2-D lat/lon.
    - Uses time selection only if there is a real time dimension; otherwise skips time.
    """
    if not nc_path or nc_path == "-" or not Path(nc_path).exists():
        return {}

    ds = xr.open_dataset(nc_path)

    # ---- find lat/lon names ----
    lat_name = next((n for n in ["latitude","lat","nav_lat"] if n in ds.variables or n in ds.coords), None)
    lon_name = next((n for n in ["longitude","lon","nav_lon"] if n in ds.variables or n in ds.coords), None)
    if not lat_name or not lon_name:
        raise SystemExit(f"Could not find lat/lon in {list(ds.coords)} + vars {list(ds.data_vars)}")

    # ---- collapse 2-D lat/lon (y,x) to 1-D if possible ----
    latn, lonn = None, None
    if getattr(ds[lat_name], "ndim", 1) == 2 and "y" in ds.dims and "x" in ds.dims:
        lat2d = ds[lat_name].values
        lon2d = ds[lon_name].values
        lat1d = lat2d[:, 0]
        lon1d = lon2d[0, :]
        if np.allclose(lat2d, lat1d[:, None], equal_nan=True) and np.allclose(lon2d, lon1d[None, :], equal_nan=True):
            ds = ds.assign_coords(y=("y", lat1d), x=("x", lon1d))
            latn, lonn = "y", "x"
        else:
            raise SystemExit(
                "Wind/field file has non-separable 2-D lat/lon. Re-download as NetCDF with regular 1-D latitude/longitude "
                "(in NOMADS g2subset choose Output: NetCDF), or use ERA5."
            )

    # Pick 1-D coord names (normal case or after collapsing)
    def pick1d(ds, cands):
        for c in cands:
            if (c in ds.coords or c in ds.dims) and getattr(ds[c], "ndim", 1) == 1:
                return c
        return None

    if latn is None: latn = pick1d(ds, ["latitude","lat","y"])
    if lonn is None: lonn = pick1d(ds, ["longitude","lon","x"])
    if latn is None or lonn is None:
        raise SystemExit(f"No 1-D lat/lon coords. Coords: {list(ds.coords)} | Dims: {list(ds.dims)}")

    # ---- time handling ----
    # prefer a real time *dimension*; if none, we won't select by time
    tdim = None
    for cand in ("time","valid_time","step"):
        if cand in ds.dims and getattr(ds[cand], "ndim", 1) == 1:
            tdim = cand
            break

    # reduce to single time if time dim exists but length==1
    if tdim and ds.dims.get(tdim, 0) == 1:
        ds = ds.isel({tdim: 0})
        tdim = None  # no need to select by time anymore

    # keep only variables we actually have
    avail = [v for v in wanted if v in ds.variables]
    if not avail:
        return {}

    # sort coords if 1-D
    for c in ([tdim] if tdim else []) + [latn, lonn]:
        if c and c in ds.coords and getattr(ds[c], "ndim", 1) == 1:
            ds = ds.sortby(c)

    # build selection target
    target = {
        latn: xr.DataArray(ais_df["lat"].values, dims="points"),
        lonn: xr.DataArray(ais_df["lon"].values, dims="points"),
    }
    if tdim:
        # vectorized nearest on time too
        target[tdim] = xr.DataArray(ais_df["ts_utc"].values, dims="points")

    # select
    sampled = ds[avail].sel(target, method="nearest")
    return {v: sampled[v].values for v in avail}


def main(a):
    if len(a) != 6:
        raise SystemExit("Usage: python join_all.py ais.csv waves.nc currents.nc wind10m.nc out.csv")
    ais_csv, waves_nc, curr_nc, wind_nc, out_csv = a[1:]

    # AIS
    ais = pd.read_csv(ais_csv)
    ais = ais.rename(columns={"timestamp":"ts_utc","Latitude":"lat","Longitude":"lon","SOG":"sog","COG":"cog"})
    for col in ("ts_utc","lat","lon"): 
        if col not in ais.columns: raise SystemExit(f"Missing AIS column: {col}")
    ais["ts_utc"] = pd.to_datetime(ais["ts_utc"], utc=True).dt.tz_localize(None)

    # Sample
    waves = sample_vars_to_points(waves_nc, ais, wanted=["VHM0","VMDR","VTM10","VTPK"])
    currs = sample_vars_to_points(curr_nc,  ais, wanted=["uo","vo","eastward_current","northward_current"])
    wind  = sample_vars_to_points(wind_nc,  ais, wanted=["u10","v10","UGRD_10maboveground","VGRD_10maboveground"])

    # Build output
    out = ais.copy()
    for k,v in waves.items(): out[k] = v
    for k,v in currs.items(): out[k] = v
    for k,v in wind.items():  out[k] = v

    # Standardize names (wind & currents aliases)
    if "UGRD_10maboveground" in out: out = out.rename(columns={"UGRD_10maboveground":"u10"})
    if "VGRD_10maboveground" in out: out = out.rename(columns={"VGRD_10maboveground":"v10"})
    if "eastward_current" in out:    out = out.rename(columns={"eastward_current":"uo"})
    if "northward_current" in out:   out = out.rename(columns={"northward_current":"vo"})

    # Derived: waves
    if "VHM0" in out:
        out["stormy"] = (out["VHM0"] >= 3.0).astype(int)
    if "VMDR" in out and "cog" in out:
        wave_to = (out["VMDR"] + 180) % 360
        ang = np.abs((wave_to - out["cog"]) % 360)
        out["rel_wave_angle"] = np.where(ang>180, 360-ang, ang)
        out["sea_sector"] = pd.cut(out["rel_wave_angle"], [-0.1,30,150,180],
                                   labels=["following","beam","head"])

    # Derived: currents
    if "uo" in out and "vo" in out and "cog" in out:
        theta = np.deg2rad(out["cog"])
        hx, hy = np.sin(theta), np.cos(theta)
        along_ms = out["uo"]*hx + out["vo"]*hy
        cross_ms = -out["uo"]*hy + out["vo"]*hx
        out["along_current_kn"] = along_ms * 1.94384
        out["cross_current_kn"] = cross_ms * 1.94384
        if "sog" in out:
            out["stw_est_kn"] = out["sog"] - out["along_current_kn"]

    # Derived: wind
    if "u10" in out and "v10" in out:
        out["wind_speed_ms"] = np.hypot(out["u10"], out["v10"])
        # wind-to direction in deg true (0=N, 90=E)
        out["wind_dir_to"] = (np.degrees(np.arctan2(out["u10"], out["v10"])) + 360) % 360
        if "cog" in out:
            wang = (out["wind_dir_to"] - out["cog"]) % 360
            out["rel_wind_angle"] = np.where(wang>180, 360-wang, wang)

    out.to_csv(out_csv, index=False)
    print("Wrote", out_csv)

if __name__ == "__main__":
    main(sys.argv)
