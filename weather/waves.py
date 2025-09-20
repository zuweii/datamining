# pip install copernicusmarine

from copernicusmarine import subset
subset(
    dataset_id="cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
    variables=["VHM0","VMDR","VTM10"],  
    minimum_longitude=102, maximum_longitude=106,
    minimum_latitude=-1,  maximum_latitude=4,
    start_datetime="2025-08-20T00:00:00Z",
    end_datetime="2025-09-05T00:00:00Z",
    output_filename="waves_subset.nc"
)

