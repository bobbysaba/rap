import pickle
import pandas as pd
import datetime as dt
import xarray as xr
from siphon.catalog import TDSCatalog

# define function to get rap data (13km gridpsace)
def get_rap(res, start, end, bounds = False, variables= False, nc_path = False, dict_path = False):
    # res: accepts integer values of 13 or 20 for the desired resolution of the RAP data
    # start: list of datetime objects indicating the start day/time for each provided period
    # end: list of datetime objects indicating the end day/time for each provided period
        # start and end times should be in UTC
    # variables: list of variables that you would like to pull from the data files
        # All variables will be collected if False
        # find an example file with vars here: https://ruc.noaa.gov/ruc/ruc2vars.html
    # bounds: list of desired coordinate bounds
        # bounds are [west, east, south, north]
        # FORMATTING: lon values range from -139 to -57
        # False returns whole domain
    # save_nc: path to save nc files
        # false: no nc files are saved
    # save_dict: path to save dictionary pickle file
        # false: no pickle file is created
    # returns dictionary of rap data
    
    # determine the resoltuion URL code
    if res == 13:
        res_code = '130'
    if res == 20:
        res_code = '252'
        
    # quit the function if the resolution is not valid
    if res != 13 and res != 20:
        print('Invalid resolution provided...quitting function.')
        return()

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
            
            # create a dict layer with the day/time if needed
            if date2 not in rap_data:
                rap_data[date2] = {}
            
            # if the day/time has already been pulled, move to the next time
            if '{:02d}'.format(t.hour) in rap_data[date2]:
                continue
            
            # create the necessary dictionary layer
            rap_data[date2]['{:02d}'.format(t.hour)] = {}
            
            # try the new AND old catelogs to find the file
            try:
                cat = TDSCatalog('https://www.ncei.noaa.gov/thredds/catalog/model-rap' + res_code + 'anl/' + date1 + '/' + date2 + '/catalog.xml')
            except:
                try:
                    cat = TDSCatalog('https://www.ncei.noaa.gov/thredds/catalog/model-rap' + res_code + 'anl-old/' + date1 + '/' + date2 + '/catalog.xml')
                except:
                    print('Invalid URL on ' + t.strftime('%Y%m%d @ %H:%M:%S'))
                    continue

            # pull the data file
            try:
                ds = cat.datasets['rap_' + res_code + '_' + date2 + '_' + '{:02d}'.format(t.hour) + '00' + '_000.grb2']
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