from copernicusmarine import subset

subset(
    dataset_id="cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",  # global physics (analysis/forecast), hourly
    variables=["uo","vo"],
    minimum_longitude=102, maximum_longitude=106,  # <-- your bbox
    minimum_latitude=-1,  maximum_latitude=4,      # <-- your bbox
    start_datetime="2025-08-20T00:00:00Z",
    end_datetime="2025-08-21T00:00:00Z",
    output_filename="currents_subset.nc"
)
