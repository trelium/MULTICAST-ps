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
device_info.columns = map(str.lower, device_info.columns)
activity = pd.read_csv(activity_file)
location = pd.read_csv(location_file)
bluetooth = pd.read_csv(bluetooth_file)
bluetooth.columns = map(str.lower, bluetooth.columns)

# Merge and prepare data
nicknames = device_info[['nickname', 'user_id']]
nickname_dict = dict(zip(nicknames['user_id'], nicknames['nickname'])) #user id key, nickname value 

# Function to group data by day and count occurrences
def process_data(data, user_id):
    user_data = data[data['user_id'] == user_id].copy()
    user_data['date'] = pd.to_datetime(user_data['timestamp']).dt.date  # Extract date
    grouped = user_data.groupby('date').size().reset_index(name='count')  # Group by date
    return grouped

# Create subplots for each nickname
fig = make_subplots(rows=int(len(nickname_dict)/2)+1, cols=2, shared_xaxes=True, subplot_titles=list(nickname_dict.values()))

for i, (user_id, nickname) in enumerate(nickname_dict.items(), start=1):
    # Process data for activity, location, and Bluetooth
    activity_data = process_data(activity, user_id)
    location_data = process_data(location, user_id)
    bluetooth_data = process_data(bluetooth, user_id)

    # Combine dates for uniform x-axis
    
    all_dates = pd.date_range(
        start=min(min(activity_data['date'], default=pd.Timestamp.today()), 
                  min(location_data['date'], default=pd.Timestamp.today()), 
                  min(bluetooth_data['date'], default=pd.Timestamp.today())), 
        end=max(max(activity_data['date'], default=pd.Timestamp.today()), 
                max(location_data['date'], default=pd.Timestamp.today()), 
                max(bluetooth_data['date'], default=pd.Timestamp.today()))
    )
    

    # Create a grid for each dataset
    def create_grid(data, all_dates):
        date_index = pd.DataFrame({'date': all_dates})  # Uniform date index
        data['date'] = pd.to_datetime(data['date'])  # Ensure datetime format
        merged = date_index.merge(data, on='date', how='left').fillna(0)  # Merge with counts
        return merged['count'].values  # Return the count array
    
    location_counts = create_grid(location_data, all_dates)
    activity_counts = create_grid(activity_data, all_dates)
    bluetooth_counts = create_grid(bluetooth_data, all_dates)

    # Create heatmaps for each dataset
    fig.add_trace(
        go.Heatmap(
            z=[activity_counts],
            x=[date.strftime('%Y-%m-%d') for date in all_dates],
            y=['Activity'],
            colorscale='RdYlGn',
            colorbar=dict(title='Counts', len=0.25, y=1.0 - 0.3 * (i - 1), tickformat='.0f'),
            showscale=(i == 1)
        ),
        row=i, col=1
    )
    
    fig.add_trace(
        go.Heatmap(
            z=[location_counts],
            x=[date.strftime('%Y-%m-%d') for date in all_dates],
            y=['Location'],
            colorscale='RdYlGn',
            showscale=False
        ),
        row=i, col=1
    )
    
    fig.add_trace(
        go.Heatmap(
            z=[bluetooth_counts],
            x=[date.strftime('%Y-%m-%d') for date in all_dates],
            y=['Bluetooth'],
            colorscale='RdYlGn',
            showscale=False
        ),
        row=i, col=1
    )
    
# Update layout
fig.update_layout(
    height=300 * len(nickname_dict),  # Adjust height based on number of subplots
    title_text="Daily Data Point Counts for Users",
    xaxis_title="Date",
    yaxis_title="Data Type",
    showlegend=False
)

# Save to HTML
#fig.write_html("daily_data_counts.html", include_plotlyjs=True)
fig.write_image("daily_data_counts.png", width=1200, height=900, scale=2)

print("Plot saved as daily_data_counts.html")