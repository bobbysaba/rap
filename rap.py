import pickle
import pandas as pd
import datetime as dt
import xarray as xr
from siphon.catalog import TDSCatalog

# path if on NSSL machine
path = '/Users/robert.saba/OneDrive - University of Oklahoma/Thesis'
# path if on any other device
# path = '/Users/bobbysaba/OneDrive - University of Oklahoma/Thesis'
storm_path = path + '/Storms/'
data_path = path + '/Data/'

#%%
# define function to get rap data (13km gridpsace)
def get_rap(start, end, data_path, bounds = False, variables= False, nc_path = False, dict_path = False):
    # start: list of datetime objects indicating the start day/time for each provided period
    # end: list of datetime objects indicating the end day/time for each provided period
    # variables: list of variables that you would like to pull from the data files
        # All variables will be collected if False
        # find an example file with vars here: https://ruc.noaa.gov/ruc/ruc2vars.html
    # data_path: path to store pickle file/nc files with RAP data
    # bounds: list of desired coordinate bounds
        # bounds are [west, east, south, north]
        # FORMATTING: lon values range from -139 to -57
        # False returns whole domain
    # save_nc: path to save nc files
        # false: no nc files are saved
    # save_dict: path to save dictionary pickle file
        # false: no pickle file is created
    # returns dictionary of rap data

    # create returned dict with data
    rap_data = {}

    # add one hour to all end times to pull the final rap data file
    end = [i + dt.timedelta(hours = 1) for i in end]

    # loop through the list of times provided to pull data from that time range
    for i in range(len(start)):
        # determine the first and last time stamps for period requested
        hr_start = dt.datetime(start[i].year, start[i].month, start[i].day, start[i].hour, 0, 0)
        hr_end = dt.datetime(end[i].year, end[i].month, end[i].day, end[i].hour, 0, 0)

        # find every data timestep in the period
        timestamps = pd.date_range(hr_start, hr_end, freq = 'H')
        
        # loop through each timestamp and pull the data
        for t in timestamps:

            # format date to match THREDDS formatting
            date1 = str(t.year) + '{:02d}'.format(t.month)
            date2 = date1 + '{:02d}'.format(t.day)
            
            # create a dict layer with the day/time
            rap_data[date2] = {}
            rap_data[date2]['{:02d}'.format(t.hour)] = {}
            
            # try the new AND old catelogs to find the file
            try:
                cat = TDSCatalog('https://www.ncei.noaa.gov/thredds/catalog/model-rap130anl/' + date1 + '/' + date2 + '/catalog.xml')
            except:
                try:
                    cat = TDSCatalog('https://www.ncei.noaa.gov/thredds/catalog/model-rap130anl-old/' + date1 + '/' + date2 + '/catalog.xml')
                except:
                    print('Invalid URL on ' + t.strftime('%Y%m%d @ %H:%M:%S'))
                    continue

            # pull the data file
            try:
                ds = cat.datasets[str('rap_130_' + date2 + '_' + '{:02d}'.format(t.hour) + '00' + '_000.grb2')]
            except:
                print('No data exists on ' + t.strftime('%Y%m%d @ %H:%M:%S'))
                continue

            ncss = ds.subset()
            query = ncss.query()

            # pull all variables if none specified
            if variables == False:
                variables = [i for i in ncss.variables]
            
            # pull vars
            for v in variables:
                query.variables(v).add_lonlat()

            # subselect any provided domain
            if bounds != False:
                query.lonlat_box(bounds[0], bounds[1], bounds[2], bounds[3])

            # attain a netCDF of your variables
            data = ncss.get_data(query)
            
            # create the dictionary layer
            for v in data.variables.keys():
                rap_data[date2]['{:02d}'.format(t.hour)][v] = data[v][:]
            
            # save nc file if specified
            if nc_path != False:
                # convert to xarray dataset to save easier
                xr_nc = xr.open_dataset(xr.backends.NetCDF4DataStore(data))
                # save nc file (overwriting previous file if applicable)
                xr_nc.to_netcdf(nc_path + date2 + '_' + '{:02d}'.format(t.hour) + '.nc', mode = 'w')
            
            # close nc files
            xr_nc.close()
            
            # print the RAP data processed
            print('RAP data on ' + date2 + ' @ ' + '{:02d}'.format(t.hour) + ' has been processed.')

    # save dictionary if specified
    if dict_path != False:
        # save the dictionary to the provided path as a pickle file
        with open(dict_path + 'rap.pickle', "wb") as output_file:
            pickle.dump(rap_data, output_file)
            
    return(rap_data)
#%%

'''
start_t = dt.datetime.now()

# indentify years in my data
years = [i for i in os.listdir(storm_path) if not i.startswith('.')]

# create lists for start and end times of periods I want rap data for
start = []
end = []

# loop through each storm to get the start and end of every period
for y in years:
    mmdd = [i for i in os.listdir(storm_path + y + '/') if not i.startswith('.')]
    for md in mmdd:
        storm_num = [i for i in range(1, len([i for i in os.listdir(storm_path + y + '/' + md + '/') if not i.startswith('.')]) + 1)]
        for i in storm_num:
            # load in the lidar data for the storm
            lidar_dat = nc.Dataset(storm_path + y + '/' + md + '/' + y + md + '_' + str(i) + '.nc')

            # add the start and end times to the start and end variables
            start.append(dt.datetime.fromtimestamp(int(lidar_dat['epochtime'][0])))
            end.append(dt.datetime.fromtimestamp(int(lidar_dat['epochtime'][-1])))

# state the variables I want
variables = ['u-component_of_wind_isobaric', 'v-component_of_wind_isobaric', 'Temperature_isobaric', 'Relative_humidity_isobaric', 'Geopotential_height_isobaric']

# bounds
bounds = [-110, -90, 30, 50]

# get the data
rap_data = get_rap(start, end, variables, data_path, bounds = bounds)

print(dt.datetime.now() - start_t)
'''

# state the variables I want
start,end = [dt.datetime(2019,5,26,23,0)],[dt.datetime(2019,5,27,0,0)]
variables = ['u-component_of_wind_isobaric']
# variables = ['ShearVectorMag_0-3km','ShearVectorMag_0-6km','SRHelicity0-1km','SRHelicity0-3km']

# bounds
bounds = [-110, -90, 30, 50]

# get the data
rap_data = get_rap(start, end, data_path, bounds = bounds, nc_path = data_path, dict_path = data_path)

# print(dt.datetime.now() - start_t)
