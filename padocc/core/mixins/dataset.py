__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"

import os
from typing import Any, Callable, Union

import xarray as xr

from ..filehandlers import (CFADataset, GenericStore, KerchunkFile,
                            KerchunkStore, ZarrStore)


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

    @classmethod
    def help(cls, func: Callable = print):
        """
        Helper function to describe basic functions from this mixin

        :param func:        (Callable) provide an alternative to 'print' function
            for displaying help information.
        """
        func('Dataset Handling:')
        func(' > project.dataset - Default product Filehandler (pointer) property')
        func(' > project.dataset_attributes - Fetch metadata from the default dataset')
        func(' > project.kfile - Kerchunk Filehandler property')
        func(' > project.kstore - Kerchunk (Parquet) Filehandler property')
        func(' > project.cfa_dataset - CFA Filehandler property')
        func(' > project.zstore - Zarr Filehandler property')
        func(' > project.update_attribute() - Update an attribute within the metadata')

    def save_ds_filehandlers(self):
        """
        Save all dataset files that already exist

        Product filehandlers include kerchunk files, 
        stores (via parquet) and zarr stores. The CFA 
        filehandler is not currently editable, so is not
        included here.
        """

        if self.kfile.file_exists():
            self.kfile.close()

        # Stores automatically check if they exist already
        self.kstore.close()
        self.zstore.close()

        self.cfa_dataset.close()

    @property
    def kfile(self) -> Union[KerchunkFile,None]:
        """
        Retrieve the kfile filehandler or create if not present
        """
                
        if self._kfile is None:
            self._kfile = KerchunkFile(
                self.dir,
                self.outproduct,
                logger=self.logger,
                **self.fh_kwargs,
            )

        return self._kfile
    
    @property
    def kstore(self) -> Union[KerchunkStore,None]:
        """
        Retrieve the kstore filehandler or create if not present
        """        
        if self._kfile is None:
            self._kfile = KerchunkStore(
                self.dir,
                self.outproduct,
                logger=self.logger,
                **self.fh_kwargs,
            )

        return self._kfile
    
    @property
    def dataset(
        self
    ) -> Union[KerchunkFile, GenericStore, CFADataset, None]:
        """
        Gets the product filehandler corresponding to cloud format.

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
        Gets the product filehandler for the CFA dataset.

        The CFA filehandler is currently read-only, and can
        be used to open an xarray representation of the dataset.
        """

        if not self._cfa_dataset:
            self._cfa_dataset = CFADataset(
                self.cfa_path,
                identifier=self.proj_code,
                logger=self.logger,
                **self.fh_kwargs
            )

        return self._cfa_dataset

    @property
    def cfa_path(self) -> str:
        """
        Path to the CFA object for this project.
        """
        return f'{self.dir}/{self.proj_code}'
    
    @property
    def zstore(self) -> Union[ZarrStore, None]:
        """
        Retrieve the filehandler for the zarr store
        """
        
        if self._zstore is None:
            self._zstore = ZarrStore(
                self.dir,
                self.outproduct,
                logger=self.logger,
                **self.fh_kwargs,
            )

        return self._zstore

    def update_attribute(
            self, 
            attribute: str, 
            value: Any, 
            target: str = 'dataset',
        ) -> None:
        """
        Update an attribute within a dataset representation's metadata.

        :param attribute:   (str) The name of an attribute within the metadata
            property of the corresponding filehandler.

        :param value:       (Any) The new value to set for this attribute.

        :param target:      (str) The target product filehandler, uses the 
            generic dataset filehandler if not otherwise specified.
        """

        if hasattr(self,target):
            meta = getattr(self,target).get_meta()

        meta[attribute] = value

        getattr(self, target).set_meta(meta)
        if target != 'cfa_dataset' and self.cloud_format != 'cfa':
            # Also update the CFA dataset.
            self.cfa_dataset.set_meta(meta)

    @property
    def dataset_attributes(self) -> dict:
        """
        Fetch a dictionary of the metadata for the dataset.
        """
        return self.dataset.get_meta()