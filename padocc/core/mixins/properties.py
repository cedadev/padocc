__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

class PropertiesMixin:
    """
    Properties relating to the ProjectOperation class that
    are stored separately for convenience and easier debugging.

    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: ProjectOperation [ONLY]
    """

    def _check_override(self, key, mapper) -> str:
        """
        'Mostly' redundant function to ensure properties
        generated from the detail file are in place.

        'Override' parameters are the only editable 
        properties in the base config file once the
        Project has been created. Other properties can
        only be changed via an Operation, not by direct
        manipulation.
        """
        if self.base_cfg['override'][key] is not None:
            return self.base_cfg['override'][key]
        
        if self.detail_cfg[mapper] is not None:
            self.base_cfg['override'][key] = self.detail_cfg[mapper]
            self.base_cfg.close()
            return self.base_cfg['override'][key]
        
        return None
    
    @property
    def outpath(self):
        """
        Path to the output product. Takes
        into account the cloud format and type.
        Extension is applied via the Filehandler that this
        string is applied to.
        """
        return f'{self.dir}/{self.outproduct}'
    
    @property
    def outproduct(self):
        """
        File/directory name for the output product.
        Revision takes into account cloud format and
        type where applicable.
        """
        if self.stage == 'complete':
            return f'{self.proj_code}.{self.revision}'
        else:
            vn = f'{self.revision}a'
            if self._is_trial:
                vn = f'trial-{vn}'
            return vn
    
    @property
    def revision(self) -> str:
        """
        Revision takes into account cloud format and
        type where applicable.
        """

        if self.cloud_format is None:
            raise ValueError(
                'Cloud format not set, revision is unknown'
            )
        
        if self.file_type is not None:
            return ''.join((self.cloud_format[0],self.file_type[0],self.version_no))
        else:
            return ''.join((self.cloud_format[0],self.version_no))
        
    @property
    def version_no(self) -> str:
        """
        Get the version number from the base config file.

        This property is read-only, but currently can be
        forcibly overwritten by editing the base config.
        """
        return self.base_cfg['version_no']

    @property
    def cloud_format(self) -> str:
        """
        Check multiple options from base and detail
        configs to find the cloud format for this project.
        
        The default is to use kerchunk.
        """
        return self._check_override('cloud_type','scanned_with') or 'kerchunk'

    @cloud_format.setter
    def cloud_format(self, value):
        """
        Override the cloud format, can be used
        to switch conversion method at any time.
        """
        self.base_cfg['override']['cloud_type'] = value

    @property
    def file_type(self) -> str:
        """
        Return the file type for this project.
        """

        return self._check_override('file_type','type')
    
    @file_type.setter
    def file_type(self, value):
        """
        Override the file type determined
        during scanning, where applicable.
        
        Best example: Changing from json to parquet
        for kerchunk storage.
        """
        
        type_map = {
            'kerchunk': ['json','parq'],
            'zarr':[None],
        }
        
        if self.cloud_format in type_map:
            if value in type_map[self.cloud_format]:
                self.base_cfg['override']['file_type'] = value
            else:
                raise ValueError(
                    f'Could not set property "file_type:{value} - accepted '
                    f'values for format: {self.cloud_format} are {type_map.get(self.cloud_format,None)}.'
                )
        else:
            raise ValueError(
                f'Could not set property "file_type:{value}" - cloud format '
                f'{self.cloud_format} does not accept alternate types.'
            )

    @property
    def source_format(self) -> str:
        """
        Get the source format of the files, 
        as determined during the scanning process.

        Note: This returns the driver used in the kerchunk
        scanning process if that step has been completed.
        """
        return self.detail_cfg.get(index='driver', default='src')
    
    def minor_version_increment(self):
        """
        Use this function for when properties of the cloud file have been changed.
        """
        
        major, minor = self.version_no.split('.')
        minor = str(int(minor)+1)

        self.version_no = f'{major}.{minor}'

    def major_version_increment(self):
        """
        Use this function for major changes to the cloud file 
        - e.g. replacement of source file data.
        """
        raise NotImplementedError
    
        major, minor = self.version_no.split('.')
        major = str(int(major)+1)

        self.version_no = f'{major}.{minor}'

    def get_stac_representation(self, stac_mapping: dict) -> dict:
        """
        Gets the stac representation of this project
        according to the provided mapping.
        
        Stac mappings should follow the intended stac
        record structure, where the value of each 
        lowest-level key in the mapping is given as a tuple
        of sources for that value from the project. The last
        value in the tuple is the default parameter, if all
        sources are unavailable.
        
        E.g ```
        {
            "experiment_id": ("property@experiment_id",None),
            "source_fmt": ("detail_cfg@source_type",None)
        }
        ```
        """

        record = self._get_stac_representation(stac_mapping)

        # Add substitutions somehow

    def _get_stac_representation(self, stac_mapping: dict) -> dict:
        """
        Gets the stac representation of this project
        according to the provided mapping.
        
        Stac mappings should follow the intended stac
        record structure, where the value of each 
        lowest-level key in the mapping is given as a tuple
        of sources for that value from the project. The last
        value in the tuple is the default parameter, if all
        sources are unavailable.
        
        E.g ```
        {
            "experiment_id": ("property@experiment_id",None),
            "source_fmt": ("detail_cfg@source_type",None)
        }
        ```
        """
        record = {}
        for k, v in stac_mapping.items():
            record[k] = None
            if isinstance(v, dict):
                record[k] = self.get_stac_representation(v)
                continue
            for value in v[:-1]:
                method, prop = value.split('@')
                if method == 'property':
                    record[k] = getattr(self, prop, None)
                    continue

                # Otherwise get from file
                try:
                    q = getattr(self, method, {})
                    record[k] = q[prop]
                except KeyError:
                    pass

            # Apply default value if source not found
            if record[k] is None:
                record[k] = v[-1]
        return record