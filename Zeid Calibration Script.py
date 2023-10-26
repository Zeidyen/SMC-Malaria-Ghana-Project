import pandas as pd

# Load the datasets
actual_data_path = 'Actual_Data_Northern_Region.csv'
simulated_data_path = 'U5_PfPR_UpperWest_3.csv'

actual_data = pd.read_csv(actual_data_path)
simulated_data = pd.read_csv(simulated_data_path)

# Display the first few rows of each dataset
actual_data.head(), simulated_data.head()
def mean_squared_error(y_true, y_pred):
    return np.mean((y_true - y_pred) ** 2)

def score(sim_df, data_df, sweep_variable):
    uniq_df = sim_df.groupby(sweep_variable).size().reset_index(name='Freq')
    mse = []
    for r, row in uniq_df.iterrows():
        mask = sim_df[sweep_variable] == row[sweep_variable]
        sim_subset_df = sim_df[mask]

        comb_df = data_df.merge(sim_subset_df, on=['year', 'month'], how='left')
        mse.append(mean_squared_error(comb_df['Actual_PfPR'], comb_df['PfPR U5']))

    uniq_df['mse'] = mse

    score_df = uniq_df.groupby(sweep_variable)['mse'].mean().reset_index(name='mse')
    return score_df

# Compute the scores
scores = score(simulated_data, actual_data, 'x_Temporary_Larval_Habitat')
scores

import numpy as np
import seaborn as sns
import os

def mean_squared_error(y_true, y_pred):
    return np.mean((y_true - y_pred) ** 2)

def score(sim_df, data_df, sweep_variable):
    uniq_df = sim_df.groupby(sweep_variable).size().reset_index(name='Freq')
    mse = []
    for r, row in uniq_df.iterrows():
        mask = sim_df[sweep_variable] == row[sweep_variable]
        sim_subset_df = sim_df[mask]

        comb_df = data_df.merge(sim_subset_df, on=['year', 'month'], how='left')
        mse.append(mean_squared_error(comb_df['Actual_PfPR'], comb_df['PfPR U5']))

    uniq_df['mse'] = mse

    score_df = uniq_df.groupby(sweep_variable)['mse'].mean().reset_index(name='mse')
    return score_df

# Compute the scores
scores = score(simulated_data, actual_data, 'x_Temporary_Larval_Habitat')
scores

def plot_output(sim_df, data_df, score_df, variable):
    sim_df['date'] = pd.to_datetime([f'{y}-{m}-01' for y, m in zip(sim_df.year, sim_df.month)])
    data_df['date'] = pd.to_datetime([f'{y}-{m}-01' for y, m in zip(data_df.year, data_df.month)])
    data_df['PfPR'] = data_df['DHS_pos'] / data_df['DHS_n']
    sns.set_style('whitegrid', {'axes.linewidth': 0.5})
    fig = plt.figure(figsize=(14, 7))
    axes = [fig.add_subplot(1, 2, x + 1) for x in range(2)]

    best_fit_value = score_df[variable][score_df['mse'].idxmin()]
    for val in sim_df[variable].unique():
        plot_df = sim_df[sim_df[variable] == val]
        a = 1 if val == best_fit_value else 0.15
        axes[0].plot(plot_df['date'], plot_df['PfPR U5'], color='#FF0000', alpha=a)

    axes[0].scatter(data_df['date'].values, data_df['PfPR'], color='k')
    axes[0].set_ylabel('PfPR')
    axes[0].set_title('Observed vs Simulated (Dark red is the best fit)')

    axes[1].plot(score_df[variable], score_df['mse'], '-o', color='#FF0000', markersize=5)
    axes[1].scatter(best_fit_value, score_df['mse'].min(), s=90, color='red')
    axes[1].set_ylabel('Mean Squared Error')
    axes[1].set_xlabel('x_Temporary_Larval_Habitat')
    axes[1].set_title('Mean MSE. Lower value = better fit')

    plt.tight_layout()
    plt.show()

# Aggregate the simulated data
sim_pfpr_agg = simulated_data.groupby(['year', 'month', 'x_Temporary_Larval_Habitat'])['PfPR U5'].mean().reset_index()

# Plot the output
plot_output(sim_pfpr_agg, actual_data, scores, 'x_Temporary_Larval_Habitat')
