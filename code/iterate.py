import glob
import dask
import fsspec
import intake
import intake_esm
import xarray as xr

table_id = {
    'ta':'Amon',        # temperature
    'tas':'Amon',       # surface air temperature
    'siconc':'SImon',   # sea ice concentration [%]
    'thetao':'Omon',    # potential temperature
    'thetaoga':'Omon',  # potential temperature global average
    'tos':'Omon',       # SST
    'pr':'Amon',        # precipitation
    'mrro':'Lmon',      # runoff
    'evspsbl':'Amon',   # evaporation
    'uas':'Amon',       # zonal surface wind 
    'vas':'Amon',       # meridional surface wind
    'zos':'Omon',       # dynamic sea level
}

def subselect_sysargv(sysargv):
    """ takes sys.argv input to python file and returns subselected names of specific simulations """
    if len(list(sysargv))==4:
        source, experiment, member = sysargv[1], sysargv[2], sysargv[3]
    elif len(list(sysargv))==3:
        source, experiment, member = sysargv[1], sysargv[2], None
    elif len(list(sysargv))==2:
        source, experiment, member = sysargv[1], None, None
    elif len(list(sysargv))==1:
        source, experiment, member = None, None, None
    assert source in [None, 'EC-Earth3P-HR','EC-Earth3P']
    assert experiment in [None, 'control-1950','hist-1950','highres-future']
    assert member in [None, 'r1i1p2f1','r2i1p2f1','r3i1p2f1']
    return source, experiment, member


def zarr_df_to_ds(df):
    zarr_path = df['zarr_path'][0]
    fsmap = fsspec.get_mapper(zarr_path)
    return xr.open_dataset(fsmap, consolidated=True, use_cftime=True, engine='zarr')


# for da, src, exp, mem in IterateECE(var='thetao', cat='jasmin-nc'):
# for da, src, exp, mem in IterateECE(var='thetao', source='EC-Earth3P', cat='jasmin-nc'):
# for da, src, exp, mem in IterateECE(var='thetao', source='EC-Earth3P-HR', cat='jasmin-nc'):

class IterateECE():
    """ Iterates over EC-Earth datasets in a given catalogue
    
    example:
    `for ds in IterateECE(var='tas',
                          cat='ceda',
                          source='EC-Earth3P-HR',
                          experiment='highres-future',
                          member='r1i1p2f1')`
                          
    returns:
    da   xr.DataArray              (if only_filenames==False)
         list of filenames         (if only_filenames==True)
    """
    def __init__(self, var, cat='ceda', source=None, experiment=None, member=None, test=False, only_filenames=False, verbose=True):
        assert cat in ['ceda','knmi','jasmin-nc']
        self.cat = cat
        print(cat)
        assert var in table_id.keys()
        self.var = var
        self.tid = table_id[var]
        self.of  = only_filenames
        self.ver = verbose
        
        if source is None:
            source = ['EC-Earth3P', 'EC-Earth3P-HR']#, 'EC-Earth3', 'EC-Earth3-Veg']
        assert type(source) in [str, list]
            
        if experiment is None:
            experiment = ['control-1950','hist-1950','highres-future']
        else:
            assert type(experiment) in [str, list]
            
        if member is None:
            member = ['r1i1p2f1','r2i1p2f1','r3i1p2f1']
        else:
            assert type(member) in [str, list]
        
        # determine platform
        # load appropriate catalogue
        if self.cat=='ceda':
            fn_col = 'https://raw.githubusercontent.com/cedadev/cmip6-object-store/master/catalogs/ceda-zarr-cmip6.json'
        elif self.cat=='jasmin-nc':
            fn_col = '/home/users/ajuling/EC-Earth3-data/jasmin_nc_highresmip.json'
        elif self.cat=='ecmwf-cca-scratch':
            fn_col = '/home/ms/nl/nkaj/EC-Earth3-data/ecmwf_cca_scratch.json'
        else:
            raise ValueError(f'cat={cat} not yet implemented')
        col = intake.open_esm_datastore(fn_col)
        print(col)
        
        # catalogue dataframe
        self.df = col.search(source_id=source, table_id=self.tid, variable_id=self.var, experiment_id=experiment, member_id=member).df
        if self.ver:
            print(f'There are {len(self.df)} datasets')
#         print(self.df)
        
        self.i = 0 
        if len(self.df)==0:
            raise StopIteration
        
        
    def __iter__(self):
        return self
    
    
    def load_netcdf(self, df):
        if self.ver:
            print(df['nc_path'])
        try:
            self.da = xr.open_mfdataset(df['nc_path'], engine='h5netcdf', chunks=dict(time=12))
        except:
            self.da = xr.open_mfdataset(df['nc_path'], engine='h5netcdf', chunks=dict(time=12), combine='nested', concat_dim='time', use_cftime=True)
        self.da = self.da[self.var]
        return
    
    
    def load_dataarray(self, df):
#         assert len(df)==1
        if self.cat=='ceda':
            try:     # zarr
                self.da = zarr_df_to_ds(df)
            except:  # netcdf
                self.load_netcdf(df)
        else:        # knmi; netcdf
            self.load_netcdf(df)
        return

    
    def __next__(self):
        """ load next dataset """
        if self.i==len(self.df):
            raise StopIteration
        # check is specific member is selected
#         if member is not None:
#             if type(member)==list:
#                 if mem not in member:
#                     continue
#             elif type(member)==str:
#                 if mem!=member:
#                     continue
#             else:
#                 raise ValueError('`member` keyword argument must be list or str')
#         print(self.df.iloc[self.i])
#         print(len(self.df.iloc[self.i]))
        self.src = self.df['source_id'][self.i]
        self.exp = self.df['experiment_id'][self.i]
        self.mem = self.df['member_id'][self.i]
        if self.of:
            # in this case self.da is a list of file names that can then be iterated over, handy for HR 3D files
            self.da = glob.glob(self.df.iloc[self.i]['nc_path'])
        else:
            self.load_dataarray(self.df.iloc[self.i])
        self.i += 1
        
        # could implement annual average / selected months
                
        return self.da, self.src, self.exp, self.mem