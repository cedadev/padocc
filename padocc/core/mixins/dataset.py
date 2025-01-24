__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

from typing import Union
import xarray as xr
import os

from ..filehandlers import (
    KerchunkFile,
    KerchunkStore,
    ZarrStore,
    CFADataset,
    GenericStore
)

class DatasetHandlerMixin:
    """
    Mixin class for properties relating to opening products.
    
    This is a behavioural Mixin class and thus should not be
    directly accessed. Where possible, encapsulated classes 
    should contain all relevant parameters for their operation
    as per convention, however this is not the case for mixin
    classes. The mixin classes here will explicitly state
    where they are designed to be used, as an extension of an 
    existing class.
    
    Use case: ProjectOperation [ONLY]
    """

    @property
    def kfile(self) -> Union[KerchunkFile,None]:
        """
        Retrieve the kfile filehandler, create if not present
        """

        if self.cloud_format != 'kerchunk':
            return None
        
        if self.file_type != 'json':
            return None
                
        if self._kfile is None:
            self._kfile = KerchunkFile(
                self.dir,
                self.outproduct
            )

        return self._kfile
    
    @property
    def kstore(self) -> Union[KerchunkStore,None]:
        """
        Retrieve the kstore filehandler, create if not present
        """
        if self.cloud_format != 'kerchunk':
            return None
        
        if self.file_type == 'json':
            return None
        
        if self._kfile is None:
            self._kfile = KerchunkStore(
                self.dir,
                self.outproduct
            )

        return self._kfile
    
    @property
    def dataset(
        self
    ) -> Union[KerchunkFile,GenericStore, CFADataset, None]:
        """
        Generic dataset property, links to the correct
        cloud format, given the Project's ``cloud_format``
        property with other configurations applied.
        """
        
        if self.cloud_format is None:
            raise ValueError(
                f'Dataset for {self.proj_code} does not exist yet.'
            )
        
        if self.cloud_format == 'kerchunk':
            if self.file_type == 'parq':
                return self.kstore
            else:
                return self.kfile
        elif self.cloud_format == 'zarr':
            return self.zstore
        elif self.cloud_format == 'cfa':
            return self.cfa_dataset
        else:
            raise ValueError(
                f'Unrecognised cloud format {self.cloud_format}'
            )

    @property
    def cfa_dataset(self) -> xr.Dataset:
        """
        Retrieve a read-only xarray representation 
        of a CFA dataset
        """

        if not self._cfa_dataset:
            self._cfa_dataset = CFADataset(
                self.cfa_path,
                self.proj_code
            )

        return self._cfa_dataset

    @property
    def cfa_path(self) -> str:
        """
        Path to the CFA object for this project.
        """
        return f'{self.dir}/{self.proj_code}.nca'
    
    @property
    def zstore(self) -> Union[ZarrStore, None]:
        """
        Retrieve a filehandler for the zarr store
        """

        if self.cloud_format != 'zarr':
            return None
        
        if self._zstore is None:
            self._zstore = ZarrStore(
                self.dir,
                self.outproduct
            )

        return self._zstore

    def update_attribute(
            self, 
            attribute, 
            value, 
            target: str = 'kfile',
        ):
        """
        Update an attribute within a 
        dataset representation's metadata.
        """

        if hasattr(self,target):
            meta = getattr(self,target).get_meta()

        meta[attribute] = value

        getattr(self, target).set_meta(meta)

    @property
    def dataset_attributes(self) -> dict:
        """
        Fetch a dictionary of the metadata for the dataset
        where possible.
        """
        return self.dataset.get_meta()