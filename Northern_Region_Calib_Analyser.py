import os
import datetime
import pandas as pd
import numpy as np
import sys
import re
import random
from idmtools.entities import IAnalyzer
from idmtools.entities.simulation import Simulation
import manifest


class MonthlyPfPRAnalyzerU5(IAnalyzer):

    def __init__(self, expt_name, sweep_variables=None, working_dir='./', start_year=1950, end_year=2028,
                 burnin=None, filter_exists=False):

        super(MonthlyPfPRAnalyzerU5, self).__init__(working_dir=working_dir,
                                                    filenames=[
                                                        f"output/MalariaSummaryReport_Monthly_U5_{x}.json"
                                                        for x in range(start_year, end_year)]
                                                    )
        self.sweep_variables = sweep_variables or ["Run_Number"]
        self.expt_name = expt_name
        self.start_year = start_year
        self.end_year = end_year
        self.burnin = burnin
        self.filter_exists = filter_exists

    def filter(self, simulation):
        if self.filter_exists:
            file = os.path.join(simulation.get_path(), self.filenames[0])
            return os.path.exists(file)
        else:
            return True

    def map(self, data, simulation):

        adf = pd.DataFrame()
        for year, fname in zip(range(self.start_year, self.end_year), self.filenames):
            d = data[fname]['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]
            pfpr = [x[1] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]
            pfpr_10 = [x[2] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['PfPR by Age Bin'][:12]
            pfpr_15 = [x[3] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]
            clinical_cases = [x[1] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]
            clinical_cases_U10 = [x[2] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Clinical Incidence by Age Bin'][:12]
            clinical_cases_U15 = [x[3] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]
            severe_cases = [x[1] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]
            severe_cases_U10 = [x[2] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Annual Severe Incidence by Age Bin'][:12]
            severe_cases_U15 = [x[3] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]
            pop = [x[1] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]
            pop_10 = [x[2] for x in d]
            d = data[fname]['DataByTimeAndAgeBins']['Average Population by Age Bin'][:12]
            pop_15= [x[3] for x in d]
            d = data[fname]['DataByTime']['PfPR_2to10'][:12]
            PfPR_2to10 = d
            d = data[fname]['DataByTime']['Annual EIR'][:12]
            annualeir = d
            simdata = pd.DataFrame({'month': range(1, 13),
                                    'PfPR U5': pfpr,
                                    'PfPR U10': pfpr_10,
                                    'PfPR U15': pfpr_15,
                                    'Cases U5': clinical_cases,
                                    'Cases U10': clinical_cases_U10,
                                    'Cases U15': clinical_cases_U15,
                                    'Severe cases U5': severe_cases,
                                    'Severe cases U10': severe_cases_U10,
                                    'Severe cases U15': severe_cases_U15,
                                    'Pop U5': pop,
                                    'Pop U10': pop_10,
                                    'Pop U15': pop_15,
                                    'PfPR_2to10': PfPR_2to10,
                                    'annualeir': annualeir})
            simdata['year'] = year
            adf = pd.concat([adf, simdata])

        for sweep_var in self.sweep_variables:
            if sweep_var in simulation.tags.keys():
                try:
                    adf[sweep_var] = simulation.tags[sweep_var]
                except:
                    adf[sweep_var] = '-'.join([str(x) for x in simulation.tags[sweep_var]])
            elif sweep_var == 'Run_Number':
                adf[sweep_var] = 0

        return adf

    def reduce(self, all_data):

        selected = [data for sim, data in all_data.items()]
        if len(selected) == 0:
            print("\nWarning: No data have been returned... Exiting...")
            return

        if not os.path.exists(os.path.join(self.working_dir, self.expt_name)):
            os.mkdir(os.path.join(self.working_dir, self.expt_name))

        print(f'\nSaving outputs to: {os.path.join(self.working_dir, self.expt_name)}')

        adf = pd.concat(selected).reset_index(drop=True)
        if self.burnin is not None:
            adf = adf[adf['year'] > self.start_year + self.burnin]
        adf.to_csv((os.path.join(self.working_dir, self.expt_name, 'U5_PfPR_ClinicalIncidence.csv')), index=False)


if __name__ == "__main__":

    from idmtools.analysis.analyze_manager import AnalyzeManager
    from idmtools.core import ItemType
    from idmtools.core.platform_factory import Platform

    expts = {
        # 'week2_weather' : '2c090358-cb7b-44e5-a2fd-842a6c23a5b7'
        # 'week2_oPutputs' : '26f947c3-0770-46df-bc6a-c1c77e36f686'
        'Prediction_additional_cycle_November': 'dfb602a9-b96a-478e-95fa-8b998f141250'
        #'Prediction_p_low':'e8010789-acf6-4145-9979-bca7807186e8'
        #'Northern_burnin_prediction_2023_2024': '9c8d9fc5-738a-422b-b948-b9b89da17f30'
    }

    jdir = manifest.job_directory
    wdir = os.path.join(jdir, 'my_outputs')

    if not os.path.exists(wdir):
        os.mkdir(wdir)

    sweep_variables = ['Run_Number', 'x_Temporary_Larval_Habitat', 'scale_temp_rain', 'scale_water_veg']

    with Platform('SLURM_LOCAL', job_directory=jdir) as platform:

        for expt_name, exp_id in expts.items():
            analyzer = [MonthlyPfPRAnalyzerU5(expt_name=expt_name,
                                              start_year=2023,
                                              end_year=2027,
                                              sweep_variables=sweep_variables,
                                              working_dir=wdir)]

            # Create AnalyzerManager with required parameters
            manager = AnalyzeManager(configuration={}, ids=[(exp_id, ItemType.EXPERIMENT)],
                                     analyzers=analyzer, partial_analyze_ok=True)
            # Run analyze
            manager.analyze()

