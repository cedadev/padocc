__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from typing import Callable, Union


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

    @classmethod
    def help(cls, func: Callable = print):
        """
        Helper function displays basic functions for use.

        :param func:        (Callable) provide an alternative to 'print' function
            for displaying help information.
        """
        func('Extra Properties:')
        func(' > project.outpath - path to the output product (Kerchunk/Zarr)')
        func(' > project.outproduct - name of the output product (minus extension)')
        func(' > project.revision - Revision identifier (major + minor version plus type indicator)')
        func(' > project.version_no - Get major + minor version identifier')
        func(' > project.cloud_format[EDITABLE] - Cloud format (Kerchunk/Zarr) for this project')
        func(' > project.file_type[EDITABLE] - The file type to use (e.g JSON/parq for kerchunk).')
        func(' > project.source_format - Get the driver used by kerchunk')
        func(
            ' > project.get_stac_representation() - Provide a mapper, '
            'fills with values from the project to create a STAC record.')

    def _check_override(self, key, mapper) -> str:
        """
        Ensure properties generated from the detail file are in place.

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
    def outpath(self) -> str:
        """
        Path to the output product. 
        
        Takes into account the cloud format and type.
        Extension is applied via the Filehandler that this
        string is applied to.
        """
        return f'{self.dir}/{self.outproduct}'
    
    @property
    def complete_product(self) -> str:
        """
        Return the name of the actual dataset.

        Products are referred to by revision only
        within the project directory, but on completion
        these will be copied out of the pipeline, 
        where they are renamed with the project code
        and revision for the actual dataset.
        """
        return f'{self.proj_code}.{self.revision}'

    @property
    def outproduct(self) -> str:
        """
        File/directory name for the output product.

        Revision takes into account cloud format and
        type where applicable.
        """
        vn = f'{self.revision}a'
        if self._is_trial:
            vn = f'trial-{vn}'
        return vn
    
    @property
    def revision(self) -> str:
        """
        Revision takes into account cloud format and type.
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
        Obtain the cloud format for this project.

        Check multiple options from base and detail
        configs to find the cloud format for this project.
        The default is to use kerchunk.
        """
        return self._check_override('cloud_type','scanned_with') or 'kerchunk'

    @cloud_format.setter
    def cloud_format(self, value: str):
        """
        Reset the cloud format value.

        Override the cloud format, can be used
        to switch conversion method at any time.
        """
        self.base_cfg['override']['cloud_type'] = value
        self.file_type = None

    @property
    def file_type(self) -> str:
        """
        Return the file type for this project.
        """

        return self._check_override('file_type','type')
    
    @file_type.setter
    def file_type(self, value: Union[str, None]):
        """
        Override the file type determined during scanning.
        
        Changing from json to parquet for kerchunk storage, 
        or switching to Zarr will require changing the file type,
        to ``parq`` or None respectively.

        """
        
        type_map = {
            'kerchunk': ['json','parq'],
            'zarr':[None],
            'CFA':[None]
        }

        if value is None:
            value = type_map[self.cloud_format][0]
        
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
        Get the source format of the files.

        This is determined during the scanning process.
        Note: This returns the driver used in the kerchunk
        scanning process if that step has been completed.
        """
        return self.detail_cfg.get(index='driver', default='src')
    
    def minor_version_increment(self, addition: Union[str,None] = None):
        """
        Increment the minor x.Y number for the version.

        Use this function for when properties of the cloud file have been changed.

        :param addition:    (str) Reason for version change; attribute change or otherwise.
        """

        addition = addition or 'Minor increment.'
        
        major, minor = self.version_no.split('.')
        minor = str(int(minor)+1)

        self.version_no = f'{major}.{minor}'

        self.dataset.update_history(
            addition = f'Minor: {addition}',
            new_version = self.version_no,
        )

        # Also update the CFA dataset history.
        if self.cloud_format != 'cfa':
            self.cfa_dataset.update_history(
                addition = f'Minor: {addition}',
                new_version = self.version_no,
            )

    def major_version_increment(self):
        """
        Increment the major X.y part of the version number.

        Use this function for major changes to the cloud file 
        - e.g. replacement of source file data.
        """
        raise NotImplementedError
    
        major, minor = self.version_no.split('.')
        major = str(int(major)+1)

        self.version_no = f'{major}.{minor}'

    def get_stac_representation(self, stac_mapping: dict) -> dict:
        """
        Apply all required substitutions to the stac representation.

        :param stac_mapping:    (dict) A padocc-map-compliant dictionary
            for extracting properties into a dictionary for STAC record-making.
        """

        record = self._get_stac_representation(stac_mapping)
        return record
        # Add substitutions - currently not implemented.

    def _get_stac_representation(self, stac_mapping: dict) -> dict:
        """
        Gets the stac representation of this project.
        
        This is according to the provided mapping.
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
    
    def apply_defaults(
            self, 
            defaults: dict, 
            target: str = 'dataset',
            remove: Union[list,None] = None
        ):
        """
        Apply a default selection of attributes to a dataset.
        """

        for attr, val in defaults.items():
            self.update_attribute(attr, val, target=target)

        for attr in remove:
            self.remove_attribute(attr, target=target)

        self.save_files()