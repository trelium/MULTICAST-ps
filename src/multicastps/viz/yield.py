import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

root_dir = '/home/jmocel/trelium/MULTICAST-ps/data/mc_participants'
# File paths
device_info_file = os.path.join(root_dir, 'device_info_mcpart.csv')
activity_file = os.path.join(root_dir, 'activity.csv')
location_file = os.path.join(root_dir, 'location.csv')
bluetooth_file = os.path.join(root_dir, 'bluetooth.csv')

# Load data
device_info = pd.read_csv(device_info_file)
activity = pd.read_csv(activity_file)
location = pd.read_csv(location_file)
bluetooth = pd.read_csv(bluetooth_file)

# Merge and prepare data
nicknames = device_info[['nickname', 'USER_ID']]
nickname_dict = dict(zip(nicknames['USER_ID'], nicknames['nickname']))

# Create subplots for each nickname
fig = make_subplots(rows=len(nickname_dict), cols=1, shared_xaxes=True, subplot_titles=list(nickname_dict.values()))

for i, (user_id, nickname) in enumerate(nickname_dict.items(), start=1):
    # Filter data by user_id
    activity_data = activity[activity['user_id'] == user_id]
    location_data = location[location['user_id'] == user_id]
    bluetooth_data = bluetooth[bluetooth['USER_ID'] == user_id]

    # Add traces for activity, location, and Bluetooth timelines
    fig.add_trace(
        go.Scatter(x=activity_data['timestamp'], y=['Activity'] * len(activity_data),
                   mode='markers', name=f'{nickname} Activity',
                   marker=dict(color='red'), showlegend=(i == 1)),
        row=i, col=1
    )

    fig.add_trace(
        go.Scatter(x=location_data['timestamp'], y=['Location'] * len(location_data),
                   mode='markers', name=f'{nickname} Location',
                   marker=dict(color='green'), showlegend=(i == 1)),
        row=i, col=1
    )

    fig.add_trace(
        go.Scatter(x=bluetooth_data['TIMESTAMP'], y=['Bluetooth'] * len(bluetooth_data),
                   mode='markers', name=f'{nickname} Bluetooth',
                   marker=dict(color='blue'), showlegend=(i == 1)),
        row=i, col=1
    )
    #fig.update_layout()

# Update layout
fig.update_layout(
    height=300 * len(nickname_dict),  # Adjust height based on number of subplots
    title_text="Interactive Timelines for Users",
    xaxis_title="Timestamp",
    yaxis_title="Data Type",
    showlegend=True
)

# Save to HTML
fig.write_html("interactive_timelines_scatter.html")

print("Plot saved as interactive_timelines.html")
