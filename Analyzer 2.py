def analyze_pickup(pickup_id=pickup_id, output_folder=output_folder,                               #
                   sweep_variables = sweep_variables,                                              #
                   EventCounter = True,
                   InsetChart = True, channels_inset_chart=channels_inset_chart, start_year = 2001,# 
                   MonthlyReport = True, report_years=report_years):                               # 
    from analyzers.habitat_analyzer import InsetChartAnalyzer, MonthlyAnalyzer                     # 
    from analyzers.EventAnalyzer import EventCounterAnalyzer
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
        if(InsetChart == True):                                                                    # Analyze InsetChart 
            analyzer_set.append(InsetChartAnalyzer(expt_name=exp_name,                             #   -> InsetChartAnalyzer from habitat_analyzer
                                                   channels=channels_inset_chart,                  #
                                                   sweep_variables=sweep_variables,                #
                                                   working_dir=output_folder,                      #
                                                   start_year = start_year,                        #
                                                   years_to_keep = burnin_years))                  #
        if(MonthlyReport == True):                                                                 # Analyze Monthly MalariaSummaryReport
            analyzer_set.append(MonthlyAnalyzer(expt_name = exp_name,                              #   -> MonthlyAnalyzer from habitat_analyzer
                                                sweep_variables=sweep_variables,                   #
                                                working_dir=output_folder,                         #
                                                years_to_keep=report_years))                       #
        if(EventCounter == True): 
            demo = pd.read_csv(os.path.join(manifest.input_dir,site_name,'demographics.csv'))
            nodes = [n for n in demo['node_id']]
            analyzer_set.append(EventCounterAnalyzer(exp_name = exp_name,
                                                     exp_id = pickup_id,
                                                     sweep_variables=sweep_variables,
                                                     nodes = nodes, 
                                                     events = events_to_record,
                                                     working_dir = output_folder,
                                                     start_day = (pickup_years-report_years)*365)) #
            
        manager = AnalyzeManager(configuration={},ids=[(pickup_id, ItemType.EXPERIMENT)],          # Create AnalyzerManager with required parameters
                                 analyzers=analyzer_set, partial_analyze_ok=True)                  #
        manager.analyze()                                                                          # Run analyze
    return                       
