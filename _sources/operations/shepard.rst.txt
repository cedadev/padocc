The SHEPARD Module
==================

The latest development in the PADOCC package is the SHEPARD Module (coming in 2025).

SHEPARD (Serial Handler for Enabling PADOCC Aggregations via Recurrent Deployment) is a component
designed as an entrypoint script within the PADOCC environment to automate the operation of the pipeline.
Groups of datasets (called ``flocks``) can be created by producing input files and placing them in a persistent
directory accessible to a deployment of PADOCC. The deployment operates an hourly check in this directory, 
and picks up any added files or any changes to existing files. The groups specified can then be automatically 
run through all sections of the pipeline.

The deployment of SHEPARD at CEDA involves a Kubernetes Pod that has access to the JASMIN filesystem as well as
the capability to deploy to JASMIN's LOTUS cluster for job submissions. The idea will be for SHEPARD to run 
continuously, slowly processing large sections of the CEDA archive and creating cloud formats that can be utilised
by other packages like DataPoint (see the Inspiration tab) that provide fast access to data.