#########################################################################################
# Import Modules ########################################################################
#########################################################################################
import os                                                                               #
import pandas as pd                                                                     #
from get_climate import get_climate                                                     #
import itertools                                                                        #
from itertools import product                                                           #
import pathlib                                                                          #
import numpy as np                                                                      #
from functools import partial                                                           #
##### idmtools ##########################################################################
from idmtools.assets import Asset, AssetCollection                                      # 
from idmtools.builders import SimulationBuilder                                         #
from idmtools.core.platform_factory import Platform                                     #
from idmtools.entities.experiment import Experiment                                     #
from idmtools_calibra.utilities.ll_calculators import beta_binomial                     #
from idmtools_calibra.utilities.ll_calculators import gamma_poisson                     #
##### emodpy ############################################################################                                  
from emodpy.emod_task import EMODTask                                                   #
from emodpy.utils import EradicationBambooBuilds                                        #
from emodpy.bamboo import get_model_files                                               #
import emod_api.config.default_from_schema_no_validation as dfs                         #
import emod_api.campaign as camp                                                        #
import emod_api.demographics.PreDefinedDistributions as Distributions                   #
from emod_api.demographics.DemographicsTemplates import CrudeRate                       #
##### emodpy-malaria ####################################################################
import emodpy_malaria.demographics.MalariaDemographics as Demographics                  #
from emodpy_malaria.reporters.builtin import *                                          #
from emodpy_malaria.interventions.scale_larval_habitats import add_scale_larval_habitats#
import emodpy_malaria.interventions.treatment_seeking as cm                             #
import emodpy_malaria.interventions.usage_dependent_bednet as itn                       #
import emodpy_malaria.interventions.irs as irs                                          #
import emodpy_malaria.interventions.drug_campaign as dc                                 #
import emodpy_malaria.interventions.diag_survey as ds                                   #
import emodpy_malaria.malaria_config as malaria_config                                  #
##### from manifest #####################################################################
import manifest                                                                         #
##### from utils_slurm ##################################################################
from utils_slurm import build_burnin_df                                                 #
#########################################################################################
#########################################################################################
#########################################################################################

######################################################################################################################
# EXPERIMENT LISTING #################################################################################################
######################################################################################################################
# experiment_id - description (site, duration, step, sweep variables, etc.)
######################################################################################################################
##### Site 'tobias' ##################################################################################################
# example_2node: 50 year burnin with migration between 2 nodes (climate 2019-2019)
#   - burnin: 0fb17c8f-9c58-4bce-927b-5d1b369fd21b
#   - pickup: ec971065-3c31-42cc-b628-0dcd06525bed 
# 
##### Site 'sapone' ##################################################################################################
# hab_sweep_1: 50 year burnin with xTLH 1.25-1.75; scale_temp_rain 5-50; scale_water_veg 0-5 (climate 2010-2019)
#   - burnin: c0b37c6e-e8d7-4949-8997-bf08ed89dc57 
#   - pickup: b7f7d059-31ca-4d6d-ba90-110588925d5e
# hab_sweep_2: 50 year burnin with xTLH 1.5-2.05; scale_temp_rain 5-50; scale_water_veg 0-5 (climate 2010-2019, 30 degrees)
#   - burnin: 1ad4f2c1-8f92-4753-bbda-e3e03437108b 
#   - pickup: dafd4104-e37e-4b55-8827-a7899c5098eb
# hab_sweep_3: 50 year burnin with xTLH 1.85-2.15; scale_temp_rain 5-100; scale_water_veg 0-2.5 (climate 2010-2019, 30 degrees)
# 7ed8f94d-6823-43ee-97e4-760d7402d559 
#   - burnin: 7ed8f94d-6823-43ee-97e4-760d7402d559
#   - pickup: 1e0df0da-aade-472b-b34c-1143c828453c 
# hab_sweep_4: 50 year burnin with xTLH 1.85-2.15; scale_temp_rain 5-100; scale_water_veg 0-2.5 (climate 2018-2018, 30 degrees)
#   - burnin: 41993724-b880-446e-bd9b-369dc1a35f25
#   - pickup: ae8b69b3-995b-4485-9ba0-41aa1a233233
# hab_sweep_5: 50 year burnin with xTLH 1.95-2.25; scale_temp_rain 10-200; scale_water_veg 0-0.5 (climate 2018-2018, 30 degrees)
#   - burnin: 4364c246-606b-4d30-a9dc-56657680df70
#   - pickup: 59b760ae-592c-4350-96d3-719f22a04a4a
# hab_sweep_6: 50 year burnin with xTLH 1.95-2.25; scale_temp_rain 10-200; scale_water_veg 0-0.5 (climate 2018-2018, 27 degrees) - updated birthRate 30 -> 38
#   - burnin: 0a691c24-9401-4cca-9a99-6f271c74764e
#   - pickup: 4f2aeb03-e762-4d26-96f6-a81896be79f8
#   - pickup: 4bdff146-c576-4855-ba0e-3055655e1e2e (with ECCM - to test, 1 seed)
# hab_sweep_7: 50 year burnin with xTLH 1.95-2.25; scale_temp_rain 10-200; scale_water_veg 0-0.5 (climate 2018-2018, 27 degrees) - updated birthRate 30 -> 38
#   - burnin: 4ddc94a3-8fb0-439c-b769-bcf3ecc2f27e
#   - pickup: 1fc31f4d-c9dc-41ca-bb80-8c20f0c1f0b6 
# test_age_distribution_analyzer 
#   - burnin: c6249a91-d4b1-4fec-895d-f9fe7bcaad00
#   - pickup: ee104986-388c-4abf-9f6c-881ab43f7816
# hab_sweep_8:
#   - burnin: 4a165db0-1a5b-4410-8c61-598cbe6c87f8
#   - pickup:
#
######################################################################################################################
######################################################################################################################
######################################################################################################################



#########################################################################
# OPTIONS ###############################################################
#########################################################################
site_name = 'sapone'                                                    # Existing or new site to request climate data for
birth_rate= 38.0                                                        # Birth Rate (per 1,000) to apply for FIXED_BIRTH_RATE, which is default in the simulation below
step = 'pickup'                                                      # Which step of the process: setup_climate? setup_migration? 
                                                                        # burnin? pickup? calibration? (also analyze_burnin for burnin < 10 years)
burnin_years = 50                                                       #
pickup_years = 10                                                       #
num_seeds = 10                                                           # number of random seeds / stochastic realizations in pickup
memory_limit = 6000                                                     # in MB, limit set for EMOD config parameter and QUEST resource allocation 
##### Experiment Specifications #########################################
exp_label = 'hab_test_8'                                                 # added label for outputs
burnin_id = '4a165db0-1a5b-4410-8c61-598cbe6c87f8'                      # Single-Node Ex. hab_sweep_1 (site 'sapone' with climate from 2018)
pickup_id = ''                      # 
exp_name = "_".join((site_name,exp_label))                              # Label metadata "experiment_name"
##### Input files #######################################################
vector_file = 'vectors.csv'                                             # Vector species relative abundance and habitat accessibility
######################################################### Migration #####
migration_files ={# migration_type & migration_filename w/out extension #
                  "migration_type":['LOCAL_MIGRATION',                  #  migration_type options: 'LOCAL_MIGRATION REGIONAL_MIGRATION' 
                                    'LOCAL_MIGRATION'],                 #  
                  "migration_file":['local_migration',                  #  corresponding filenames for humans and vectors in simulation_inputs/migration
                                    'vector_local_migration']}          #
migration_files = pd.DataFrame(migration_files)                         # Row 0 = human local migration, Row 1 = vector local migration
##################################################### Interventions #####
cm_file = 'case_management.csv'                                         # Case management: coverage by age
smc_file = 'SMC.csv'                                                    # SMC: coverage by round
itn_file = 'ITN.csv'                                                    # Bednets: distribution coverages and insecticide properties
itn_age_file = 'ITN_age.csv'                                            # Bednets: Age-specific usage
itn_season_file = 'ITN_season.csv'                                      # Bednets: Seasonal usage
irs_file = 'IRS.csv'                                                    # IRS Housing Modification (scheduled)
fever_screening_file = 'ECCM.csv'                                       #
############################################## Reference data files ##### 
incidence_file = 'monthly_cases.csv'                         # Incidence data to match to Monthly SummaryReport
pcr_prevalence_file = 'pcr_prevalence_allAge.csv'                       # Prevalence data to match to InsetChart
###### Outputs ##########################################################
events_to_record = ["Received_SMC",                                     # Events to Record in Custom_Individual_Events [] and ReportEventCounter(Event_Trigger_List[])
                    "Received_Treatment",                               #
                    "Bednet_Got_New_One",                               #
                    "Bednet_Using",                                     #
                    "Bednet_Discarded"]                             # 
sweep_variables = ['x_Temporary_Larval_Habitat',                        # Grouping variables for outputs (must be in simulation metadata)
                   'scale_constant',                                    #
                   'scale_temp_rain',                                   #
                   'scale_water_veg']                                   #   
channels_inset_chart = ['Statistical Population',                       # Output variables to include from InsetChart (and/or SpatialReport?)
                        'Daily Bites per Human',                        #
                        'Daily EIR',                                    #
                        'Rainfall',                                     # 
                        'Air Temperature',                              #
                        'PCR Parasite Prevalence']                           #
output_folder = os.path.join('simulation_output',site_name,exp_label)   # Folder to store outputs in
os.makedirs(output_folder,exist_ok=True)                                #
report_years = 5                                                        # Limit to final n years of pickup in non-InsetChart reports of pickup
##### Climate ###########################################################
######################################################## Date range #####
climate_start = "2018"                                                  # From Day 1 of this year ...
climate_end = "2018"                                                    # ... to Day 365 of this year
####################################################### Adjustments #####
temperature = 27.0                                                      # Set Constant Temperature. 'None' will use ERA5 temperature series
rain_shift = None                                                       # Shift rainfall series by n days (**in testing)
##### Larval Habitat Space ##############################################
scale_water_veg = list(np.logspace(0,0.75,9))   + [0]                            # Amount of Water_Vegetation relative to Constant 
scale_temp_rain = list(np.logspace(0,3,15))                    # Amount of Temporary_Rainfall relative to Constant 
scale_constant = [1.0]                                                  # Amount of Constant relative to Base of 5e5. Typically left as [1.0] 
xTLH_range = list(np.logspace(0.1,0.9,20))                            # Note: x_Temporary_Larval_Habitat scales ALL habitat types, including constant
                                                                        # 
habitat_df = pd.DataFrame(zip(product(scale_constant,                   # Generate or refresh habitat dataframe
                                      scale_temp_rain,                  #
                                      scale_water_veg)))                #
n_habs = int(len(habitat_df))                                           #
#########################################################################
#########################################################################
#########################################################################

############################################################################################################################
# SIMULATION FUNCTIONS #####################################################################################################
############################################################################################################################ #######################
def scale_habitat_from_df(df=habitat_df, index=-1, vdf=None):                                                              # scale_habitat_from_df()
    habitat_mix=tuple([1,1,1])                                                                                             # Default to base amounts of constant, temp_rain, and water_veg
    if(index>=0):                                                                                                          # #######################
        habitat_mixes= habitat_df                                                                                          #
        habitat_mix = habitat_mixes[0][index]                                                                              # Sets scale-factors based on row of habitat_df                                                                                             
    #lh_scales=pd.DataFrame({'CONSTANT': [habitat_mix[0]],                                                                 #
    #                        'TEMPORARY_RAINFALL': [habitat_mix[1]],                                                       #
    #                        'WATER_VEGETATION': [habitat_mix[2]]})                                                        #
    if vdf is not None:                                                                                                    # Use supplied vector dataframe to scale vector proportions in each node
        all_scales = {'NodeID': vdf['node_id'].unique()}                                                                   #
        s = vdf['species'].unique()                                                                                        #
        for species in s:                                                                                                  #
            sub_vdf = vdf[vdf['species'] == species]                                                                       # For each species
            all_scales[f"CONSTANT.{species}"] = [habitat_mix[0] * f for f in sub_vdf['fraction']]                          #   - Constant * scale_constant * fraction_species_in_node
            all_scales[f"TEMPORARY_RAINFALL.{species}"] = [habitat_mix[1] * f for f in sub_vdf['fraction']]                #   - Temporary_Rainfall * scale_temp_rain * fraction_species_in_node
            all_scales[f"WATER_VEGETATION.{species}"] = [habitat_mix[2] * f for f in sub_vdf['fraction']]                  #   - Water_Vegetation * scale_water_veg * fraction_species_in_node
        lh_scales=pd.DataFrame(all_scales)                                                                                 #
                                                                                                                           #
    return lh_scales                                                                                                       # Return dataframe of scale factors
                                                                                                                           #
                                                                                                                           # ###########
def set_param(simulation, param, value):                                                                                   # set_param() - to manually set specific EMOD parameters
                                                                                                                           # ###########
    return simulation.task.set_parameter(param,value)                                                                      #   
                                                                                                                           #
                                                                                                                           # ##############
def set_param_fn(config, vector_file=vector_file):                                                                         # set_param_fn() - To setup config file    
    import emodpy_malaria.malaria_config as conf                                                                           # ##############  
    config = conf.set_team_defaults(config, manifest)                                                                      # Use team defaults  
    ##### Set logLevels to suppress/allow output ###########################################################################
    config.parameters['logLevel_JsonConfigurable'] = 'ERROR'                                                               # 
    config.parameters['logLevel_VectorHabitat'] = 'ERROR'                                                                  #           
    config.parameters['logLevel_StandardEventCoordinator'] = 'ERROR'                                                       # 
    config.parameters['logLevel_SusceptibilityMalaria'] = 'ERROR'                                                          #     
    config.parameters['logLevel_default'] = 'ERROR'                                                                        #
    config.parameters['logLevel_Eradication'] = 'ERROR'                                                                    #
    ##### Set Memory Limits  ############################################################################################### 
    config.parameters.Memory_Usage_Halting_Threshold_Working_Set_MB = int(memory_limit)                                    # To kill job
    config.parameters.Memory_Usage_Warning_Threshold_Working_Set_MB = int(memory_limit)-500                                # To give warning
    ##### Add Climate ######################################################################################################
    config.parameters.Air_Temperature_Filename = os.path.join('climate','dtk_15arcmin_air_temperature_daily_revised.bin')  # Air temperature
    config.parameters.Land_Temperature_Filename = os.path.join('climate','dtk_15arcmin_air_temperature_daily_revised.bin') # Land temperature (**use air temperature file**)
    config.parameters.Rainfall_Filename = os.path.join('climate','dtk_15arcmin_rainfall_daily_revised.bin')                # Rainfall
    config.parameters.Relative_Humidity_Filename=os.path.join('climate','dtk_15arcmin_relative_humidity_daily_revised.bin')# Relative Humidity
    ##### Other Config #####################################################################################################
    config.parameters.Run_Number = 0                                                                                       #
    config.parameters.x_Temporary_Larval_Habitat = 1                                                                       #
    ##### Serialization ####################################################################################################
    if(step=="burnin"):                                                                                                    #
        config.parameters.Serialized_Population_Writing_Type = "TIMESTEP"                                                  #
        config.parameters.Serialization_Time_Steps = [365 * burnin_years]                                                  # By default, serializes population at the very end of the burnin
        config.parameters.Serialization_Mask_Node_Write = 0                                                                #
        config.parameters.Serialization_Precision = "REDUCED"                                                              #
        config.parameters.Simulation_Duration = burnin_years*365                                                           #
    if(step=="pickup"):                                                                                                    #
        config.parameters.Serialized_Population_Reading_Type = "READ"                                                      #
        config.parameters.Serialization_Time_Steps = [365 * burnin_years]                                                  # Should match Serialization_Time_Steps defined for burnin
        config.parameters.Serialization_Mask_Node_Read = 0                                                                 #
        config.parameters.Simulation_Duration = pickup_years*365+30                                                        # (+30) is specific to allow run through end of January in year following
    ##### Outputs ##########################################################################################################
    config.parameters.Enable_Default_Reporting = 1                                                                         # Produce InsetChart by default
    if step == 'burnin' and burnin_years > 10:                                                                             #
        config.parameters.Enable_Default_Reporting = 0                                                                     # Turn off InsetChart for burnin
    if step == 'pickup':                                                                                                   #
        config.parameters.Custom_Individual_Events = events_to_record                                                      # Custom events for reporting during pickup
    ##### Migration ########################################################################################################
    if os.path.exists(os.path.join("migration",".".join((migration_files['migration_file'][0],'bin')))):
        #config.parameters.Enable_Migration = 1                                                                                                                       #
        config.parameters.Migration_Model = "FIXED_RATE_MIGRATION"
        config.parameters.Migration_Pattern = "SINGLE_ROUND_TRIPS"
        config.parameters.Enable_Migration_Heterogeneity = 0     
        config.parameters.Enable_Local_Migration = 1
        config.parameters.Local_Migration_Roundtrip_Duration = 2
        config.parameters.Local_Migration_Roundtrip_Probability = 1.0
        config.parameters.Local_Migration_Filename = os.path.join("migration",".".join((migration_files['migration_file'][0],'bin')))
    if os.path.exists(os.path.join("migration",".".join((migration_files['migration_file'][1],'bin')))):
        config.parameters.Enable_Vector_Migration = 1
        config.parameters.Enable_Vector_Migration_Local=1
        config.parameters.Vector_Migration_Filename_Local = os.path.join("migration",".".join((migration_files['migration_file'][1],'bin')))
    ##### Vector Config ####################################################################################################
    vdf = pd.read_csv(os.path.join(manifest.input_dir,site_name,vector_file))                                              #
    vdf = vdf[vdf['node_id']== vdf['node_id'].unique()[0]]                                                                 #
    s = [species for species in vdf['species']]                                                                            # Get list of species from vector_file
    conf.add_species(config, manifest, s)                                                                                  # Add species to config
    ##################################################################################################### Habitat ##########
    base_1=1e7                                                                                                             # Default amount for constant
    base_2=1e7                                                                                                             # Default amount for temporary_rainfall and water_vegetation 
    for r in range(len(s)):                                                                                                # For each species, scale habitat availability according to vector_file flag
        conf.set_species_param(config,                                                                                     #
                               species= vdf['species'][r],                                                                 #
                               parameter="Habitats",                                                                       #
                               value= [{"Habitat_Type": "CONSTANT",                                                        #
                                        "Max_Larval_Capacity": base_1 * vdf['constant'][r]},                               #
                                       {"Habitat_Type": "TEMPORARY_RAINFALL",                                              #
                                        "Max_Larval_Capacity":base_2  * vdf['temp_rain'][r]},                              #
                                       {"Habitat_Type": "WATER_VEGETATION",                                                #
                                        "Max_Larval_Capacity":base_2  * vdf['water_veg'][r]}],                             #
                               overwrite=True)                                                                             # delete previous habitat types 
    return config                                                                                                          #
                                                                                                                           # ############
def build_camp(habitat=-1, scale_start_day=1):                                                                             # build_camp() - to generate campaign file. 
    camp.set_schema(manifest.schema_file)                                                                                  # ############           
    vector_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,vector_file))                                        #                                                                 # 
    lh_scales= scale_habitat_from_df(df=habitat_df,vdf=vector_df,index=habitat)                                            #
    add_scale_larval_habitats(camp, df=lh_scales, start_day=scale_start_day, repetitions=-1)                               # scale habitats by TYPE for all species according to habitat_df (sweep) and vector_df (observed fraction)
    ##### Case management ##################################################################################################                             
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"interventions",cm_file)):                                 #
        cm_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",cm_file))                            # Read cm_file
        nodes = [n for n in cm_df['node_id'].unique()]                                                                     #
        for node in nodes:                                                                                                 # For each node...
            cm_df = cm_df[cm_df['node_id']==node]                                                                          #
            cm_df = cm_df[cm_df['phase']==step].reset_index()                                                              # Filter to burnin or pickup#            
                                                                                                                           #
            for year in cm_df['year']:                                                                                     # For each year...
                  sub_df = cm_df[cm_df['year'] == year].reset_index()                                                      #
                  targets = [] 
                  #print(year)                                                                                             #
                  for r in range(len(sub_df)) :                                                                            # ... Build a set of targets from rows of cm_file ...
                      cm_coverage_by_age =  {'trigger': str(sub_df['trigger'][r]),                                         #
                                             'coverage': float(sub_df['coverage'][r]),                                     #
                                             'agemin': float(sub_df['age_min'][r]),                                        #
                                             'agemax': float(sub_df['age_max'][r]),                                        #
                                             'seek': float(sub_df['seek'][r]),                                             #
                                             'rate': float(sub_df['rate'][r])}                                             #
                      targets.append(cm_coverage_by_age)                                                                   #
                  #print(targets)
                  cm.add_treatment_seeking(camp, node_ids = [int(node)],                                                   # ... Add treatment seeking to simulation.
                                           start_day = int(sub_df['start_day'][0]),                                        #
                                           duration = int(sub_df['duration'][0]),                                          #
                                           drug=['Artemether','Lumefantrine'],                                             #   **Hard-Coded treatment with AL**
                                           targets=targets,                                                                #
                                           broadcast_event_name="Received_Treatment")                                      #
    ##### ITNs - Usage Dependent by AGE and SEASON #########################################################################
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"interventions",itn_file)):                                #
        itn_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions", itn_file))                         # Read itn_file
        nodes = [n for n in itn_df['node_id'].unique()]
        for node in nodes:
            itn_df = itn_df[itn_df['node_id']==node]
            itn_df = itn_df[itn_df['phase']==step].reset_index()                                                           # Filter to burnin or pickup
            if len(itn_df) > 0:                                                                                            #
                itn_age = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",itn_age_file))             # Read age dependence file
                itn_season = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",itn_season_file))       # Read seasonal dependence file - **currently assumes same seasonal pattern for all years**
                itn_seasonal_usage = {"Times": list(itn_season['season_time']),                                            #
                                      "Values":list(itn_season['season_usage'])}                                           #
                for year in itn_df['year']:                                                                                # For each year ...
                    sub_df = itn_df[itn_df['year']==year].reset_index()                                                    #
                    itn_discard_config = {"Expiration_Period_Distribution": "WEIBULL_DISTRIBUTION",                        # ...Set discard distribution based on itn_file
                                          "Expiration_Period_Kappa": float(sub_df['discard_k'][0]),                        #
                                          "Expiration_Period_Lambda": float(sub_df['discard_l'][0])}                       #
                    itn_age_year = itn_age[itn_age['year']==year]                                                          # ...Set age-dependence
                    itn_age_bins = itn_age_year['age']                                                                     #
                    itn_age_usage = itn_age_year['age_usage']                                                              #
                    itn.add_scheduled_usage_dependent_bednet(camp, node_ids = [int(node)],                                 # ...Add scheduled usage-dependent bednets with specified:
                                                             intervention_name = "UsageDependentBednet",                   # 
                                                             start_day = int(sub_df['start_day'][0]),                      # - distribution timing 
                                                             demographic_coverage = float(sub_df['coverage'][0]),          # - distribution coverage
                                                     			   killing_initial_effect = float(sub_df['kill_effect'][0]),     # - insecticide properties
                                                     			   killing_decay_time_constant = int(sub_df['kill_decay'][0]),   # 
                                                     			   blocking_initial_effect = float(sub_df['block_effect'][0]),   # 
                                                     			   blocking_decay_time_constant=int(sub_df['block_decay'][0]),   # 
                                                             age_dependence = {"Times": list(itn_age_bins),                # - Age effect on usage 
                                                                               "Values": list(itn_age_usage)},             #
                                                             seasonal_dependence = itn_seasonal_usage,                     # - Seasonal effect on usage
                                                             discard_config = itn_discard_config)                          # - Discard probability
    ##### SMC ##############################################################################################################
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"interventions",smc_file)):                                #
        smc_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",smc_file), encoding='latin')        # Read smc_file
        nodes = [n for n in smc_df['node_id'].unique()]                                                                    #
        for node in nodes:                                                                                                 #
            smc_df = smc_df[smc_df['node_id']==node]                                                                       #
            smc_df = smc_df[smc_df['phase'] == step].reset_index()                                                         # Filter to burnin or pickup
            if len(smc_df) > 0:                                                                                            #
                for r in range(len(smc_df)):                                                                               # For each row (round) in smc_file...
                    dc.add_drug_campaign(camp, campaign_type="MDA", drug_code="SPA", node_ids =[int(node)],                     # ... Add SMC with drug SPA
                                         start_days=[int(smc_df['start_day'][r])],                                         # ... on this day
                                         repetitions=1,                                                                    # ... once
                                         coverage=float(smc_df['coverage'][r]),                                            # ... at this coverage
                                         target_group={'agemin': 0.25, 'agemax': 5},                                       # ... for children age 0.25 to 5
                                         receiving_drugs_event_name="Received_SMC")                                        #
                    dc.add_drug_campaign(camp, campaign_type="MDA", drug_code="SPA", node_ids = [int(node)],                    # and ... Add SMC "leak" for children ages 5 to 6
                                         start_days=[int(smc_df['start_day'][r])],                                         #
                                         repetitions = 1,                                                                  #
                                         coverage=float(smc_df['coverage'][r]) * 0.5,                                      # Older children have half the coverage **(hard-coded)**     
                                         target_group={'agemin': 5, 'agemax': 6},                                          # Ages 5-6
                                         receiving_drugs_event_name="Received_SMC")                                        #
    ##### IRS ##############################################################################################################
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"interventions",irs_file)):                                #
        irs_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",irs_file))                          # read irs_file
        nodes = [n for n in irs_df['node_id'].unique()]                                                                    #
        for node in nodes:                                                                                                 #
            irs_df = irs_df[irs_df['node_id']==node]                                                                       #        
            irs_df = irs_df[irs_df['phase']==step].reset_index()                                                           # filter to burnin or pickup
            if len(irs_df) > 0 :                                                                                           #
                for r in range(len(irs_df)):                                                                               # Add IRS by year from each row of irs_df
                    irs.add_scheduled_irs_housing_modification(camp, node_ids=[int(node)],                                 #
                                                               start_day=int(irs_df['start_day'][r]),                      #
                                                               demographic_coverage=float(irs_df['coverage'][r]),          #
                                                               killing_initial_effect=float(irs_df['kill_effect'][r]),     #
                                                               killing_box_duration=int(irs_df['kill_duration'][r]),       #
                                                               killing_decay_time_constant=int(irs_df['kill_decay'][r]),   #
                                                               repelling_initial_effect=float(irs_df['repel_effect'][r]),  #
                                                               repelling_box_duration= int(irs_df['repel_duration'][r]),   #
                                                               repelling_decay_time_constant=int(irs_df['repel_decay'][r]))#
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"interventions",fever_screening_file)):
        fever_screen_df = pd.read_csv(os.path.join(manifest.input_dir,site_name,"interventions",fever_screening_file))
        
        nodes = [n for n in fever_screen_df['node_id'].unique()]
        for node in nodes:
            fever_screen_df = fever_screen_df[fever_screen_df['node_id']==node]
            fever_screen_df = fever_screen_df[fever_screen_df['phase']==step].reset_index()
            if len(fever_screen_df) > 0:
                for r in range(len(fever_screen_df)):
                    ds.add_diagnostic_survey(camp, node_ids=[int(node)],
                                             coverage=float(fever_screen_df['coverage'][r]), 
                                             repetitions= int(fever_screen_df['rounds'][r]), 
                                             tsteps_btwn_repetitions=int(fever_screen_df['frequency'][r]), 
                                             start_day=int(fever_screen_df['sim_day'][r]),
                                             diagnostic_type=str(fever_screen_df['diagnostic'][r]), 
                                             diagnostic_threshold=float(fever_screen_df['threshold'][r]), 
                                             measurement_sensitivity=float(fever_screen_df['sensitivity'][r]), 
                                             event_name=str(fever_screen_df['event_name'][r]), 
                                             positive_diagnosis_configs=[{"class": "BroadcastEvent",
                                                                          "Broadcast_Event": str(fever_screen_df['positive_test_event_name'][r])}],
                                             received_test_event=str(fever_screen_df['received_test_event_name'][r]))
                
                    dc.add_drug_campaign(camp, node_ids=[int(node)], 
                                         campaign_type = str(fever_screen_df['campaign'][r]), 
                                         drug_code = str(fever_screen_df['drug_code'][r]),
                                         coverage=float(fever_screen_df['coverage2']), 
                                         start_days = [int(fever_screen_df['sim_day'][r])],
                                         diagnostic_type = str(fever_screen_df['diagnostic2'][r]), 
                                         diagnostic_threshold=float(fever_screen_df['threshold2'][r]),
                                         trigger_condition_list = [str(fever_screen_df['positive_test_event_name'][r])],
                                         receiving_drugs_event_name="Fever_Screen_Treated")
            
    return camp                                                                                                            #
                                                                                                                           # #####################################
def update_campaign_multiple_parameters(simulation, habitat_index, scale_start_day):                                       # update_campaign_multiple_parameters()
    build_campaign_partial = partial(build_camp, habitat = int(habitat_index), scale_start_day=int(scale_start_day))       # #####################################
    simulation.task.create_campaign_from_callback(build_campaign_partial)                                                  #
    return {"scale_constant": float(habitat_df[0][habitat_index][0]),                                                      #
            "scale_temp_rain": float(habitat_df[0][habitat_index][1]),                                                     #
            "scale_water_veg": float(habitat_df[0][habitat_index][2])}                                                     #
                                                                                                                           # #################################
def update_serialize_parameters(simulation, df, x: int):                                                                   # update_serialization_parameters()
    """                                                                                                                    # #################################
    This function connects populations and important parameters between burnin and pickup simulations                      # For row x of burnin_df...
    """                                                                                                                    #
    path = df["serialized_file_path"][x]                                                                                   # Path to serialized population file 
    xTLH = float(df["x_Temporary_Larval_Habitat"][x])                                                                      # ...x_Temporary_Larval_habitat from burnin
    scale_constant = float(df["scale_constant"][x])                                                                        # ...scale_constant from burnin
    scale_temp_rain = float(df["scale_temp_rain"][x])                                                                      # ...scale_temp_rain from burnin
    scale_water_veg = float(df['scale_water_veg'][x])                                                                      # ...scale_water_veg from burnin
    h_index =  habitat_df.index[habitat_df[0]==(scale_constant,scale_temp_rain,scale_water_veg)][0]                        # -> Find matching habitat_index -> 
    simulation.task.set_parameter("Serialized_Population_Filenames", df["Serialized_Population_Filenames"][x])             # Set serialized population filename in pickup
    simulation.task.set_parameter("Serialized_Population_Path", os.path.join(path, "output"))                              # Set serialized population file path
    simulation.task.set_parameter("x_Temporary_Larval_Habitat", xTLH)                                                      # Set x_Temporary_Larval_habitat in pickup
    update_campaign_multiple_parameters(simulation, habitat_index = h_index, scale_start_day = 1)                          # Set habitat scale_factors in pickup
    return {"x_Temporary_Larval_Habitat":xTLH,                                                                             # Return serialized parameters as tags for analysis
            "scale_constant":scale_constant,                                                                               #
            "scale_temp_rain": scale_temp_rain,                                                                            #
            "scale_water_veg": scale_water_veg}                                                                            #
                                                                                                                           # #############
def build_demog():                                                                                                         # build_demog()
    """                                                                                                                    # #############
    This function builds a demographics input file for the DTK using emod_api.                                             #
    """                                                                                                                    #
    demo_file = os.path.join(manifest.input_dir,site_name,'demographics.csv')                                              #  
    demog = Demographics.from_csv(input_file = demo_file, id_ref=site_name,                                                # Generate demographics from demo_file
                                  init_prev = 0.01, include_biting_heterogeneity = True)                                   #
    #demo_df = pd.read_csv(demo_file)                                                                                      #
    demog.SetBirthRate(CrudeRate(birth_rate*1000))                                                                         # Set Birth Rate (** currently need to x 1,000 for FIXED_BIRTH_RATE)
    demog.SetMortalityRate(CrudeRate(birth_rate))                                                                          # Set Mortality Rate (daily probability of dying)
    demog.SetEquilibriumAgeDistFromBirthAndMortRates(CrudeRate(birth_rate),CrudeRate(birth_rate))                          # Set Age Distribution using unscaled Birth Rate for Births and Deaths
    return demog                                                                                                           #
                                                                                                                           # #############  
def general_sim(selected_platform):                                                                                        # general_sim() - connecting all of the simulation steps
    """                                                                                                                    # #############
    This function is designed to be a parameterized version of the sequence of things we do                                #
    every time we run an emod experiment.                                                                                  #
    """                                                                                                                    #
    ##### Platform #########################################################################################################
    if(step=="burnin"):                                                                                                    # Specifications for burnin simulations 
        platform = Platform(selected_platform, job_directory=manifest.job_directory,                                       # 
                            partition='b1139', account='b1139',                                                            # Use b1139 for longer simulations 
                            time='6:00:00',max_running_jobs=200, mem=memory_limit,                                         # (do not exceed 100 max_running_jobs)
                            modules=['singularity'])                                                                       #
    if(step=="pickup"):                                                                                                    # Specifications for pickup simulations
        platform = Platform(selected_platform, job_directory=manifest.job_directory,                                       # 
                            partition='short', account='p30781',                                                           # Use p30781 if you have access for shorter simulations
                            time='4:00:00',max_running_jobs=2000, mem=memory_limit,                                        #
                            modules=['singularity'])                                                                       #
    ##### Task #############################################################################################################
    print("Creating EMODTask (from files)...")                                                                             #
    task = EMODTask.from_default2(config_path="config.json",                                                               #
                                  eradication_path=manifest.eradication_path,                                              #
                                  campaign_builder=build_camp,                                                             #
                                  schema_path=manifest.schema_file,                                                        #
                                  param_custom_cb=set_param_fn,                                                            #
                                  ep4_custom_cb=None,                                                                      #
                                  demog_builder=build_demog,                                                               #
                                  plugin_report=None)                                                                      #
    task.set_sif(manifest.SIF_PATH, platform)                                                                              # set the singularity image to be used when running this experiment #
    task.config.parameters.Birth_Rate_Dependence = "FIXED_BIRTH_RATE"                                                      # Set birth rate dependence
    task.common_assets.add_directory(os.path.join(manifest.input_dir,                                                      # add weather directory as an asset
                                                  site_name, "climate",                                                    #
                                                  "".join((climate_start,"001","-",climate_end,"365")),                    #
                                                  str("_".join(("",str(temperature),"degrees")))),                         #
                                     relative_path="climate")                                                              #
    if os.path.exists(os.path.join(manifest.input_dir,site_name,"migration")):                                             #
        task.common_assets.add_directory(os.path.join(manifest.input_dir,site_name,"migration"),                           # add migration files as an asset
                                         relative_path="migration")                                                        # 
    ##### Builder ##########################################################################################################
    builder = SimulationBuilder()                                                                                          # 
    if(step =='burnin'):                                                                                                   # Sweeping during the burnin:
        builder.add_sweep_definition(partial(set_param, param='x_Temporary_Larval_Habitat'), xTLH_range)                   # Scale ALL habitats by x_Temporary_Larval_Habitat
        builder.add_multiple_parameter_sweep_definition(update_campaign_multiple_parameters,                               # Scale specific habitats according to habitat_df[index]
                                                        dict(scale_start_day = [1], habitat_index = list(range(n_habs))))  #
    if(step=='pickup'):                                                                                                    # Sweeping during the pickup:
        burnin_df = build_burnin_df(burnin_id, platform, burnin_years*365)                                                 # Grab serialized data from burnin 
        builder.add_sweep_definition(partial(update_serialize_parameters, df=burnin_df), range(len(burnin_df.index)))      # Match parameters from burnin in pickup
        builder.add_sweep_definition(partial(set_param, param='Run_Number'), range(num_seeds))                             # Sweep over Run_Number
    #### Reports ###########################################################################################################
    if(step == 'burnin'):                                                                                                  # Burnin Reports:
        start_report=(burnin_years-report_years)*365                                                                       #
        end_report = burnin_years*365                                                                                      #
        #add_malaria_summary_report(task, manifest, start_day=start_report, end_day=end_report,                             # - Malaria Summary Report 
        #                           reporting_interval=30, age_bins=[0.25,85],                                              #
        #                           pretty_format = True,                                                                   #
        #                           filename_suffix = 'Monthly_AllAge',                                                     #
        #                           max_number_reports=report_years*12+1)                                                   #
                                                                                                                           #
    if(step == 'pickup'):                                                                                                  # Pickup Reports:
        start_report = (pickup_years-report_years)*365                                                                     # 
        end_report = pickup_years*365+30                                                                                   # **(+30 to catch January of following year)**
        add_malaria_summary_report(task, manifest,                                                                         # - Malaria Summary Report 
                                   start_day=start_report,                                                                 #
                                   end_day=end_report,                                                                     #
                                   reporting_interval=30,                                                                  #   - monthly (30 days) 
                                   age_bins=[0.25, 85],                                                                    #   - Ages 0.25 to 85 (based on INDIE study range)
                                   pretty_format=True,                                                                     #
                                   filename_suffix='Monthly_AllAge',                                                       #
                                   max_number_reports=report_years*12+1)                                                   # **(+1 to catch January of following year)** 
        add_malaria_summary_report(task, manifest, start_day = (pickup_years-1)*365, end_day=pickup_years*365,                 #  - Malaria Summary Report for Age Distribution
                                   reporting_interval=1,                                                                   # - daily
                                   age_bins = [0.5,1,2,5,10,15,20,25,50,75,100],                                           # - age bins of interest (max)
                                   max_number_reports=365,
                                   pretty_format=True, filename_suffix='Daily') 
        
        demo = pd.read_csv(os.path.join(manifest.input_dir,site_name,'demographics.csv'))
        for n in demo['node_id'].unique(): 
            add_report_event_counter(task, manifest,
                                     node_ids = [int(n)],  filename_suffix="_".join(('node',str(n))),                             # - Report Event Counter (by node)
                                     start_day = start_report,                                                             #
                                     end_day = end_report,                                                                 #
                                     event_trigger_list = events_to_record,                                                #    
                                     min_age_years = 0,                                                                    #    - Minimum Age
                                     max_age_years = 85)                                                                   #    - Maximum Age
        if os.path.exists(os.path.join(manifest.input_dir,site_name,"migration")):                                         #
            add_human_migration_tracking(task,manifest)                                                                    # - Human Migration tracking report
    ##### Run Experiment ###################################################################################################
    experiment = Experiment.from_builder(builder,task, name=exp_name)                                                      #
    experiment.run(wait_until_done=False, platform=platform)                                                               #
############################################################################################################################
############################################################################################################################
############################################################################################################################

    
####################################################################################################
# CALIBRATION FUNCTIONS ############################################################################
####################################################################################################       
from idmtools_calibra.utilities.ll_calculators import beta_binomial                                #
##### Analyzer(s) ################################################################################## 
def analyze_pickup(pickup_id=pickup_id, output_folder=output_folder,                               #
                   sweep_variables = sweep_variables,                                              #
                   EventCounter = True,
                   InsetChart = True, channels_inset_chart=channels_inset_chart, start_year = 2010,# 
                   MonthlyReport = True, report_years=report_years,
                   AgeReport = True):                                                              # 
    from idmtools.entities import IAnalyzer	                                                       #
    from idmtools.entities.simulation import Simulation                                            #
    from idmtools.analysis.analyze_manager import AnalyzeManager                                   #
    from idmtools.core import ItemType                                                             #
    from idmtools.core.platform_factory import Platform                                            #
    import datetime                                                                                #
    print('Analyzing ', pickup_id)                                                                 #
    jdir =  manifest.job_directory                                                                 # Set job directory - where is experiment?
    with Platform('SLURM_LOCAL',job_directory=jdir) as platform:                                   # 
        analyzer_set = []                                                                          #
        if(InsetChart == True):     
            from analyzers.habitat_analyzer import InsetChartAnalyzer                              # Analyze InsetChart 
            analyzer_set.append(InsetChartAnalyzer(expt_name=exp_name,                             #   -> InsetChartAnalyzer from habitat_analyzer
                                                   channels=channels_inset_chart,                  #
                                                   sweep_variables=sweep_variables,                #
                                                   working_dir=output_folder,                      #
                                                   start_year = start_year,                        #
                                                   years_to_keep = burnin_years))                  #
        if(MonthlyReport == True):                                                                 # Analyze Monthly MalariaSummaryReport
            from analyzers.habitat_analyzer import MonthlyAnalyzer                                 #
            analyzer_set.append(MonthlyAnalyzer(expt_name = exp_name,                              #   -> MonthlyAnalyzer from habitat_analyzer
                                                sweep_variables=sweep_variables,                   #
                                                working_dir=output_folder,                         #
                                                years_to_keep=report_years))                       #
        if(EventCounter == True):                                                                  # Analyze EventCounter Report
            from analyzers.EventAnalyzer import EventCounterAnalyzer
            demo = pd.read_csv(os.path.join(manifest.input_dir,site_name,'demographics.csv'))
            nodes = [n for n in demo['node_id']]
            analyzer_set.append(EventCounterAnalyzer(exp_name = exp_name,
                                                     exp_id = pickup_id,
                                                     sweep_variables=sweep_variables,
                                                     nodes = nodes, 
                                                     events = events_to_record,
                                                     working_dir = output_folder,
                                                     start_day = (pickup_years-report_years)*365)) #
        if(AgeReport == True):                                                                     # Analyze Daily MalariaSummaryReport for Pop by Age
            from analyzers.habitat_analyzer import AgeDistributionAnalyzer                         #
            analyzer_set.append(AgeDistributionAnalyzer(expt_name=exp_name,                        #   -> AgeDistributionAnalyzer from habitat_analyzer
                                                        sweep_variables=sweep_variables,           #
                                                        working_dir=output_folder))                #
        manager = AnalyzeManager(configuration={},ids=[(pickup_id, ItemType.EXPERIMENT)],          # Create AnalyzerManager with required parameters
                                 analyzers=analyzer_set, partial_analyze_ok=True)                  #
        manager.analyze()                                                                          # Run analyze
    return                                                                                         #
##### Scoring ######################################################################################
def score_analyzed_pickup():                                                                       #
    print('Scoring ', pickup_id)                                                                   #
    ####################################################### All-Age Monthly Clinical Incidence #####
    ref_inc = pd.read_csv(os.path.join(manifest.input_dir,site_name,                               # Reference All-Age Monthly Clinical Incidence from incidence_file 
                                       "reference_data",incidence_file))                           #  
    ref_inc['month'] = ref_inc['month'] % 12           
    ref_inc['month'][ref_inc['month']==0] =12                                                      #   -> Convert to single monthly series
    ref_inc['cases'] = ref_inc.groupby(['month'])['clinical_cases'].transform('mean') 
    ref_inc['population'] = ref_inc.groupby(['month'])['population'].transform('mean')              # 
    ref_inc = ref_inc.drop_duplicates('month',keep="first")[['month','cases','population']]        #
                                                                                                    #
    cases_min = min(ref_inc['cases'])                                                              #
    ref_inc['cases_max'] = max(ref_inc['cases'])                                                              #
    ref_inc['cases_norm'] = (ref_inc['cases'])/(ref_inc['cases_max'])                                         #   -> Normalize against maximum                                                                            
    sim_inc = pd.read_csv(os.path.join(output_folder,'AllAge_MonthlySummaryReport.csv'))           # Simulation Incidence from Monthly Malaria Summary Report
    sim_inc['month'] = sim_inc['month'] % 12
    sim_inc['month'][sim_inc['month']==0] = 12
    
    sim_inc = sim_inc.groupby(['month',
                     'x_Temporary_Larval_Habitat',
                     'scale_constant', 
                     'scale_temp_rain', 
                     'scale_water_veg'], as_index=False).agg({'Cases':'mean', 'Pop':'mean'})
    sim_inc['c_max'] = sim_inc.groupby(['x_Temporary_Larval_Habitat',
                                'scale_constant', 
                                'scale_temp_rain', 
                                'scale_water_veg'], as_index=False)['Cases'].transform('max')
    sim_inc['c_max'][sim_inc['c_max']==0] = 1
    sim_inc['c_norm'] = sim_inc['Cases']/sim_inc['c_max']
    
    comp = pd.merge(sim_inc,ref_inc, on = "month")
    score_df = []
    sv = ['month'] + sweep_variables
    uniq_df = comp.groupby(sv).size().reset_index(name="Freq")
    ll_inc = []
    for r, row in uniq_df.iterrows():

        mask0 = comp[sv[0]] == row[sv[0]]
        mask1 = comp[sv[1]] == row[sv[1]]
        mask2 = comp[sv[2]] == row[sv[2]]
        mask3 = comp[sv[3]] == row[sv[3]]
        mask4 = comp[sv[4]] == row[sv[4]]
        mask = mask1 & mask2 & mask3 & mask4 & mask0
        subset_df = comp[mask].reset_index()
        subset_df['c_norm'][subset_df['c_norm']==0] = 0.001
        ll = gamma_poisson(subset_df.population,subset_df.Pop,subset_df.cases_norm*subset_df.cases_max, subset_df.c_norm*subset_df.c_max)
        ll_inc = ll_inc + [ll]
   
    uniq_df['ll_inc'] = ll_inc
    inc_df = uniq_df.groupby(sweep_variables).agg({'ll_inc':'mean'})
    
    
    ################################################################### All-Age PCR Prevalence #####                                         
    ref_pcr_prev = pd.read_csv(os.path.join(manifest.input_dir,site_name,                          # Reference All-Age PCR Prevalence from pcr_prevalence_file (INDIE cross-sectional surveys, DHS/MIS)
                                            "reference_data",pcr_prevalence_file))                 # 
                                                                                                   #
    sim_pcr_prev = pd.read_csv(os.path.join(output_folder,'InsetChart.csv'))                       # Simulation PCR Prevalence from InsetChart
    days = [d for d in ref_pcr_prev['sim_day'].unique()]
    sim_pcr_prev = sim_pcr_prev[sim_pcr_prev['Time'].isin(days)]                                   #
    #sim_pcr_prev = sim_pcr_prev.groupby(['x_Temporary_Larval_Habitat',                            #
    #                                    'scale_constant',                                         #
    #                                    'scale_temp_rain',                                        #
    #                                    'scale_water_veg',                                        #
    #                                    'Time']).transform('mean')                                # Mean by sweep_variables
    sim_pcr_prev = sim_pcr_prev[['Time','x_Temporary_Larval_Habitat','scale_constant',
                                'scale_temp_rain', 'scale_water_veg', 'PCR Parasite Prevalence',
                                'Statistical Population']]
    sim_pcr_prev=sim_pcr_prev.rename(columns={"Time":"sim_day", 
                                 "Statistical Population":"population", 
                                 "PCR Parasite Prevalence":"pcr_parasite_prev"})                   #
    comp = pd.merge(sim_pcr_prev, ref_pcr_prev, on = "sim_day")                                    #
    #print(comp)                                                                                    #
    ########################################################################## Perform scoring #####         
    score_df = []          
    sv = ['sim_day'] + sweep_variables                                                                        # TO ADD
    uniq_df = comp.groupby(sv).size().reset_index(name='Freq')
    #print(uniq_df)
    ll_prev = []
    for r, row in uniq_df.iterrows():

        mask0 = comp[sv[0]] == row[sv[0]]
        mask1 = comp[sv[1]] == row[sv[1]]
        mask2 = comp[sv[2]] == row[sv[2]]
        mask3 = comp[sv[3]] == row[sv[3]]
        mask4 = comp[sv[4]] == row[sv[4]]
        mask = mask1 & mask2 & mask3 & mask4 & mask0
        subset_df = comp[mask].reset_index()
        #print(subset_df)
        ll_prev = ll_prev + [beta_binomial(subset_df.population, subset_df.population, 
                                 subset_df.pcr_prevalence*subset_df.population, subset_df.pcr_parasite_prev*subset_df.population)]
        #print(ll)
    #print(len(ll_prev))
    uniq_df['ll_prev'] = ll_prev
    prev_df = uniq_df.groupby(sweep_variables).agg({'ll_prev':'mean'})
    score_df = pd.merge(inc_df,prev_df, on= sweep_variables)
    print(score_df)
    score_df.to_csv(os.path.join(output_folder,"prevalence_log_likelihoods.csv"))
    
    return score_df                                                                                #
####################################################################################################
####################################################################################################
####################################################################################################


#####################################################################################################
# EXECUTION #########################################################################################
#####################################################################################################
if __name__ == "__main__":                                                                          #
    print(site_name)                                                                                #
    print(step)                                                                                     #
    if(step == 'setup_climate'):                                                                    # Get climate files from COMPS. log-in popup will appear.
        get_climate(tag=site_name, label='',                                                        #
                    start_year=climate_start, end_year = climate_end,                               #
                    rain_shift=rain_shift, fix_temp=temperature)                                    #
    if(step == 'setup_migration'):                                                                  # Construct migration input files from migration_files dictionary
        from generate_migration_files import convert_txt_to_bin                                     #
        outfile_base=os.path.join(manifest.input_dir,site_name,'migration')                         #                                                               
        for mig_type, mig_file in migration_files.items():                                          #
                convert_txt_to_bin(os.path.join(outfile_base, ".".join((mig_file,"csv"))),           #
                                   os.path.join(outfile_base, ".".join((mig_file,"bin"))),           #
                                   mig_type='%s' % mig_type.upper(),                                #
                                   id_reference=site_name)                                          #
                                                                                                    #
    if step in ('burnin','pickup'):                                                                 # Run simulations
        import emod_malaria.bootstrap as dtk                                                        #
        import pathlib                                                                              #
        dtk.setup(pathlib.Path(manifest.eradication_path).parent)                                   #
        selected_platform = "SLURM_LOCAL"                                                           #
        general_sim(selected_platform)                                                              #
    if step =='analyze_burnin':                                                                     # Analyze burnin (only during testing, otherwise no output generated)
        IC = True                                                                                   #
        if(burnin_years > 10):                                                                      #
            IC = False                                                                              #
        analyze_pickup(pickup_id=burnin_id, output_folder=output_folder,                            #
                       sweep_variables = sweep_variables,                                           #
                       InsetChart = IC,channels_inset_chart=channels_inset_chart,start_year = 1960, #
                       MonthlyReport = True, report_years=report_years,                             #
                       AgeReport= False)                                                            #
    if step in ('calibrate','calibration'):                                                         # Analyze pickup and score against calibration targets
        analyze_pickup(pickup_id=pickup_id, output_folder=output_folder,                            #
                       sweep_variables = sweep_variables,                                           #
                       InsetChart=True,channels_inset_chart=channels_inset_chart,start_year = 2010, # 
                       MonthlyReport=True, report_years=report_years,                               #
                       AgeReport=True)                                                             #
        score_analyzed_pickup()
    if step == 'score':
        score_analyzed_pickup()                                                                     #
    if (step == 'print_summary'):                                                                   # Print summary of sim specs
        print('Site:', site_name)                                                                   #
        print('Step:', step)                                                                        #
        print('Nodes:')                                                                             # ... **To-Do: Add more**
        print(pd.Data.Frame(os.path.join(manifest.input_dir,site_name,'demographics.csv')))         #
#####################################################################################################
#####################################################################################################
#####################################################################################################
