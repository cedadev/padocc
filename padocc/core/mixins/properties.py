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
        if self.base_cfg['override'][key] is not None:
            return self.base_cfg['override'][key]
        
        if self.detail_cfg[mapper] is not None:
            self.base_cfg['override'][key] = self.detail_cfg[mapper]
            self.base_cfg.close()
            return self.base_cfg['override'][key]
        
        return None
    
    @property
    def outpath(self):
        return f'{self.dir}/{self.outproduct}'
    
    @property
    def outproduct(self):
        if self.stage == 'complete':
            return f'{self.proj_code}.{self.revision}'
        else:
            vn = f'{self.revision}a'
            if self._is_trial:
                vn = f'trial-{vn}'
            return vn
    
    @property
    def revision(self) -> str:

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

        return self.base_cfg['version_no']

    @property
    def cloud_format(self) -> str:
        return self._check_override('cloud_type','scanned_with') or 'kerchunk'

    @cloud_format.setter
    def cloud_format(self, value):
        self.base_cfg['override']['cloud_type'] = value

    @property
    def file_type(self) -> str:
        """
        Return the file type for this project.
        """

        return self._check_override('file_type','type')
    
    @file_type.setter
    def file_type(self, value):
        
        type_map = {
            'kerchunk': ['json','parq'],
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
        return self.detail_cfg.get(index='driver', default=None)
    
    def minor_version_increment(self):
        """
        Use this function for when properties of the cloud file have been changed."""
        
        major, minor = self.version_no.split('.')
        minor = str(int(minor)+1)

        self.version_no = f'{major}.{minor}'

    def major_version_increment(self):
        """
        Use this function for major changes to the cloud file 
        - e.g. replacement of source file data."""
        raise NotImplementedError
    
        major, minor = self.version_no.split('.')
        major = str(int(major)+1)

        self.version_no = f'{major}.{minor}'
