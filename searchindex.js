Search.setIndex({docnames:["assess","assess-overview","compute","errors","examples","execution","execution-source","index","init","scan","validate"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":4,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,sphinx:56},filenames:["assess.rst","assess-overview.rst","compute.rst","errors.rst","examples.rst","execution.rst","execution-source.rst","index.rst","init.rst","scan.rst","validate.rst"],objects:{"":[[0,0,0,"-","assess"],[6,0,0,"-","group_run"],[6,0,0,"-","single_run"]],"pipeline.compute":[[2,0,0,"-","serial_process"]],"pipeline.compute.serial_process":[[2,2,1,"","Converter"],[2,4,1,"","KerchunkDriverFatalError"],[2,1,1,"","init_logger"]],"pipeline.compute.serial_process.Converter":[[2,3,1,"","hdf5_to_zarr"],[2,3,1,"","ncf3_to_zarr"],[2,3,1,"","tiff_to_zarr"]],"pipeline.errors":[[3,4,1,"","ChunkDataError"],[3,4,1,"","ExpectTimeoutError"],[3,4,1,"","FilecapExceededError"],[3,4,1,"","MissingKerchunkError"],[3,4,1,"","MissingVariableError"],[3,4,1,"","NoOverwriteError"],[3,4,1,"","NoValidTimeSlicesError"],[3,4,1,"","ProjectCodeError"],[3,4,1,"","ShapeMismatchError"],[3,4,1,"","SoftfailBypassError"],[3,4,1,"","TrueShapeValidationError"],[3,4,1,"","ValidationError"],[3,4,1,"","VariableMismatchError"]],"pipeline.init":[[8,1,1,"","get_input"],[8,1,1,"","get_proj_code"],[8,1,1,"","get_removals"],[8,1,1,"","get_updates"],[8,1,1,"","init_config"],[8,1,1,"","load_from_input_file"],[8,1,1,"","make_dirs"],[8,1,1,"","make_filelist"],[8,1,1,"","text_file_to_csv"]],"pipeline.scan":[[9,1,1,"","eval_sizes"],[9,1,1,"","format_float"],[9,1,1,"","format_seconds"],[9,1,1,"","get_internals"],[9,1,1,"","get_seconds"],[9,1,1,"","map_to_kerchunk"],[9,1,1,"","perform_safe_calculations"],[9,1,1,"","safe_format"],[9,1,1,"","scan_config"],[9,1,1,"","scan_dataset"]],"pipeline.validate":[[10,1,1,"","compare_data"],[10,1,1,"","find_dimensions"],[10,1,1,"","get_netcdf_list"],[10,1,1,"","get_vslice"],[10,1,1,"","locate_kerchunk"],[10,1,1,"","match_timestamp"],[10,1,1,"","open_kerchunk"],[10,1,1,"","open_netcdfs"],[10,1,1,"","pick_index"],[10,1,1,"","run_backtrack"],[10,1,1,"","run_successful"],[10,1,1,"","validate_data"],[10,1,1,"","validate_dataset"],[10,1,1,"","validate_selection"],[10,1,1,"","validate_shapes"],[10,1,1,"","validate_timestep"]],assess:[[0,1,1,"","assess_main"],[0,1,1,"","cleanup"],[0,1,1,"","error_check"],[0,1,1,"","extract_keys"],[0,1,1,"","find_codes"],[0,1,1,"","format_str"],[0,1,1,"","get_code_from_val"],[0,1,1,"","output_check"],[0,1,1,"","progress_check"],[0,1,1,"","save_sel"],[0,1,1,"","show_options"]],group_run:[[6,1,1,"","get_attribute"],[6,1,1,"","get_group_len"],[6,1,1,"","main"]],pipeline:[[3,0,0,"-","errors"],[8,0,0,"-","init"],[9,0,0,"-","scan"],[10,0,0,"-","validate"]],single_run:[[6,1,1,"","get_proj_code"],[6,1,1,"","main"],[6,1,1,"","run_compute"],[6,1,1,"","run_init"],[6,1,1,"","run_scan"],[6,1,1,"","run_validation"]]},objnames:{"0":["py","module","Python module"],"1":["py","function","Python function"],"2":["py","class","Python class"],"3":["py","method","Python method"],"4":["py","exception","Python exception"]},objtypes:{"0":"py:module","1":"py:function","2":"py:class","3":"py:method","4":"py:exception"},terms:{"0":[3,6],"00":4,"01":4,"04":4,"05":4,"06":4,"07":4,"08":4,"09":4,"1":[5,6,10],"10":4,"100":10,"11":4,"12":4,"12km":4,"12x15":4,"13":4,"15":4,"180":4,"1a":10,"2":7,"30":4,"boolean":0,"byte":9,"case":[4,10],"class":2,"final":8,"function":[0,6,10],"int":[0,9,10],"new":[0,5,10],"return":10,"switch":0,"true":4,"try":10,"var":[3,6],"while":5,A:4,For:4,If:[4,5,10],In:10,No:[3,10],Not:[0,10],The:[4,7],To:4,Will:10,_:5,__ensemble_member__:4,__frequency__:4,__variable_id__:4,__version__:4,abid:0,about:7,access:9,account:10,across:[0,10],activ:5,ad:7,add:4,addit:7,all:[0,2,4,5,9,10],allot:4,allow:5,an:[7,10],appropri:8,ar:[0,7,10],archiv:7,arg:[0,6,8,9,10],argument:[5,6],arrai:[0,10],assembl:[6,10],assess:[7,10],assess_main:0,assessor:7,associ:8,assur:5,attempt:9,attribut:[4,8],b:5,badc:4,base:[7,8],belong:0,below:4,bin:5,blank:4,bool:0,box:10,builder:7,bypass:[3,5,10],bypass_err:2,cach:10,calcul:[9,10],can:[4,10],catalog:7,central:7,chang:4,check:[0,5],checker:0,chunkdataerror:3,cleantyp:0,cleanup:0,clear:0,clt:4,cmip6:8,code:[0,4,5,6,7,8,10],collat:9,collect:0,come:9,command:[0,8],compar:[0,10],compare_data:10,comparison:3,complet:[0,10],comput:[6,7],config:[0,8],configur:[2,8,9],consid:[0,4],consist:7,contain:[4,5,10],content:0,convers:2,convert:[2,8,9],correct:[0,6,7],correspond:0,could:7,cpf:9,creat:[4,5,7,8],csv:[4,8],csvfile:4,ctype:9,current:[0,3,7,10],currentdiv:10,d:5,dai:4,data:[4,9,10],dataset:[0,6,7,10],debug:0,defin:10,deriv:10,detail:4,determin:[0,8,10],dict:0,dictionari:0,differ:[0,4,7],dimens:10,dimlen:10,dir:5,directori:[0,4,5,8,10],disk:9,div:10,diverg:0,divis:10,document:10,domain:4,done:5,driver:2,dry:5,dryrun:5,dtype:10,dure:9,e:[4,5],each:[0,4],easi:7,easiest:4,efil:0,elementwis:10,end:[4,10],ensemble_memb:4,ensur:[7,10],env:6,environ:6,equal:10,equat:10,equival:10,err:5,error:[0,4,5,7,9,10],error_check:0,eval_s:9,examin:0,exampl:7,except:[2,3],exclud:5,execut:7,exit:[5,10],expect:10,expecttimeouterror:3,exponenti:10,extract:[0,4],extract_kei:0,f:5,fail:[0,2,3,4,5],failur:3,fals:[2,10],fatal:3,fetch:0,file:[0,2,3,4,5,6,7,8,9,10],filecapexceedederror:3,filepath:[0,9],find:0,find_cod:0,find_dimens:10,first:[3,5],flag:[3,4],follow:4,forc:5,format:[0,2,4,9,10],format_float:9,format_second:9,format_str:0,found:[0,3,10],four:7,fraction:10,frequenc:4,from:[0,4,6,7,8,9,10],fstring:9,full:0,futur:7,g:[4,5],gener:0,get:[6,8,9,10],get_attribut:6,get_code_from_v:0,get_group_len:6,get_input:8,get_intern:9,get_netcdf_list:10,get_proj_cod:[6,8],get_remov:8,get_second:9,get_str:10,get_upd:8,get_vslic:10,given:[0,6,9,10],group:[0,6,7],group_run:[4,5,6],groupdir:[0,5,6],groupid:[0,5],grow:10,h:5,halt:[0,10],handler:9,have:4,hdf5:2,hdf5_to_zarr:2,hdf:7,help:5,henc:4,here:[0,7,10],histori:4,how:10,hur:4,huss:4,i:[4,5],id:[0,4,5,6],identifi:5,ignor:[0,4],implement:[0,6,10],increas:10,index:[0,7,10],individu:6,info:0,inform:[4,5],ingest:7,init:[2,4,5,7,8],init_config:8,init_logg:2,initi:5,initialis:[6,7],input:[4,5,8],involv:10,irrelev:4,isparq:10,job:[0,4,5,6],jobid:0,json:[4,10],kei:[0,4,8],kerchunk:[3,4,9,10],kerchunk_box:10,kerchunkdriverfatalerror:2,kfile:10,known:4,kobj:10,kobject:10,kvariabl:10,kwarg:2,label:[0,5],land:4,later:0,latest:10,left:4,length:[0,10],like:0,limit:5,line:[0,8],list:[0,8,9,10],load:8,load_from_input_fil:8,locate_kerchunk:10,log:[0,5],logger:[0,2,6,8,9,10],m:5,mai:[4,10],main:[0,6,8,9],make:[7,10],make_dir:8,make_filelist:8,mani:10,map:9,map_to_kerchunk:9,master:5,match:[0,4,10],match_timestamp:10,matter:4,max:10,mean:10,messag:[0,2,3,5],metadata:[4,8],method:4,min:[4,10],minimum:10,miss:3,missingkerchunkerror:3,missingvariableerror:3,mitig:9,mm:9,mode:[2,5],modul:7,move:10,multi:5,multipl:7,my_remov:4,my_upd:4,n:[5,10],name:[2,8,10],ncf3_to_zarr:2,nd:10,necessari:4,netbox:10,netcdf3:2,netcdf:[3,4,7,9,10],new_history_valu:4,new_id_valu:4,new_vers:5,next:7,nfile:[2,3,9,10],none:[0,8],nooverwriteerror:3,novalidtimesliceserror:3,number:[0,10],nviron:5,object:[0,2,10],occurr:0,older:0,one:[0,5,10],onli:4,open:10,open_kerchunk:10,open_netcdf:10,oper:0,option:[0,5],os:0,other:10,otherwis:5,output:[0,7],output_check:0,overview:7,overwrit:5,p:5,packag:7,page:7,pair:8,parallel:[6,7],paramet:[4,6],parquet:10,part:7,particular:0,pass:6,path:[0,4,5,8,10],pattern:[4,8],per:[0,10],perform:[2,5,9,10],perform_safe_calcul:9,phase:[0,5,7,9],pick:10,pick_index:10,pid:6,pipelin:[0,2,8,9,10],point:4,posit:[5,10],possibl:[4,10],pr:4,prefix:8,present:[4,5],previous:5,print:5,process:[4,5,6,9],processor:7,produc:7,progress:0,progress_check:0,proj_cod:[0,5,9],proj_dir:[5,8,9,10],project:[0,4,5,6,8,10],project_cod:4,projectcodeerror:3,proper:[4,9,10],properti:4,provid:0,prsn:4,psl:4,py:[4,5],python:[4,7],q:5,qualiti:5,r:5,randomli:10,rcm:4,rcm_12km_rcp85_01_clt_day_v20190731:4,rcp85:4,re:0,read:[6,9],real:10,record:5,recurs:10,redo_pcod:0,reflect:4,relev:[0,4],remov:[0,4,8],repeat:[0,5,10],repeat_id:[0,5,6],requir:[0,3,4],rerun:3,resolut:4,result:0,rl:4,rss:4,run:[0,4,6,7,10],run_backtrack:10,run_comput:6,run_init:6,run_scan:6,run_success:10,run_valid:6,s:[4,5],safe:9,safe_format:9,same:10,save:0,save_sel:0,savedcod:0,savetyp:0,sbatch:6,scan:[6,7,9],scan_config:9,scan_dataset:9,scenario:4,script:[5,6,8],search:7,second:[3,9],section:9,see:0,select:[0,10],serial:7,serial_process:2,set:[4,7,8],setup:6,sfcwind:4,shape:10,shapemismatcherror:3,should:4,show:5,show_opt:0,simpl:0,singl:[6,7,10],single_run:[5,6],size:[5,9,10],skip:5,slice:10,slurm:6,slurmstepd:4,snw:4,softfailbypasserror:3,softli:3,some:[0,4,7,9],sourc:[5,7,8],specif:[0,4,8,9,10],specifi:10,ss:9,stage:0,start:6,statement:5,std:5,std_var:9,step:[5,7,10],store:0,str:[0,8,9,10],string:[0,9],structur:8,stuck:0,stuff:7,sub_collect:4,subset:[5,6,10],suitabl:3,summaris:0,summat:10,t:[4,5],ta:4,take:[0,6,10],tasmax:4,tasmin:4,templat:9,tempor:10,test:[9,10],testfil:9,text:[0,8],text_file_to_csv:8,tfile:2,thi:[0,4,5,7,8,10],thing:7,thorough:[5,10],those:10,through:9,tiff:[2,7],tiff_to_zarr:2,time:[4,5,9,10],time_allow:[5,9],timestamp:10,timestep:10,tool:[0,7],total:[0,10],trueshapevalidationerror:3,tupl:0,two:4,txt:0,type:[0,2,3,4],typeerror:10,ua:4,uk:4,ukcp18:4,ukcp18_land:4,ukcp:7,ukcp_12km_dai:4,under:0,unit:9,unless:4,until:10,up:[0,4,8],updat:[4,7,8],us:[0,4,5],usag:5,v20190731:4,v:5,va:4,valid:[3,6,7],validate_data:10,validate_dataset:10,validate_select:10,validate_shap:10,validate_timestep:10,validationerror:3,valu:[0,4,8,9,10],variabl:[6,10],variable_id:4,variablemismatcherror:3,variou:0,venvpath:5,verbos:[2,3,5],verifi:10,version:[0,4,5],version_no:4,virtual:5,vname:10,volm:9,vs:10,w:5,wai:4,want:4,warn:0,we:4,when:2,which:[0,4,10],whole:0,within:[5,10],work:[0,5,7,8],workdir:[0,5,6],x12:4,x15:4,xarrai:10,xobj:10,xobject:10,xv:10,xvariabl:10,ye:10},titles:["Assess Module","Assessor Tool","Compute Module","Custom Pipeline Errors","Worked Examples","Running the Pipeline","Pipeline Execution","Welcome to the Kerchunk Pipeline documentation!","Initialisation Module","Scanner Module","Validation Module"],titleterms:{"1":4,"2":4,advanc:7,assess:0,assessor:1,comput:2,content:7,custom:3,dataset:[4,5],document:7,error:3,exampl:4,execut:6,group:[4,5],indic:7,initialis:[4,8],kerchunk:7,modul:[0,2,8,9,10],pipelin:[3,4,5,6,7],processor:2,run:5,scan:4,scanner:9,serial:2,singl:5,tabl:7,tool:1,ukcp:4,valid:10,welcom:7,work:4}})