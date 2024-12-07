import numpy as np
import kaleido
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go



def main():
    stop_frequency = pd.read_csv('stop_frequency.csv', index_col=0)
    routes_speeds = pd.read_csv('routes_speeds.csv', index_col=0)
    trips_ridership = pd.read_csv('trips_ridership.csv', index_col=0)
    
    # Average number of riders per trip by transit type
    riders_plot = px.bar(trips_ridership, x='transit_type', y='riders/trip', color='transit_type', 
             width=700, height=650, color_discrete_sequence=['#7b2d8e', '#c1d82f', '#49a942', '#005daa'],
            title='Average riders per trip', labels={'transit_type':'Transit type', 'riders/trip': 'Riders'})
    save_plot(riders_plot)
    
    # Share of ridership by transit type
    riders_pie = px.pie(trips_ridership, values='ridership', names='transit_type', color='transit_type', width=700, height=500, 
                   color_discrete_map={'WCE': '#7b2d8e', 'SeaBus': '#c1d82f', 'SkyTrain': '#49a942', 'Bus': '#005daa'},
                   title='Share of ridership by transit type')
    save_plot(riders_pie)
    
    # Share of trips by transit type
    trips_pie = px.pie(trips_ridership, values='trips', names='transit_type', color='transit_type', width=700, height=500, 
                   color_discrete_map={'WCE': '#7b2d8e', 'SeaBus': '#c1d82f', 'SkyTrain': '#49a942', 'Bus': '#005daa'},
                   title='Share of trips by transit type')
    save_plot(trips_pie)


    
    # 3 fastest routes
    fastest_routes = routes_speeds[['km/h', 'route_short_name', 'route_long_name']].head(3)
    fastest_routes['Route'] = fastest_routes['Route #'] + ' (' + fastest_routes['Route name'] + ')'
    fastest_routes['km/h'] = np.round(fastest_routes['km/h'], decimals=2)
    fastest_routes = fastest_routes.rename(columns={"route_short_name": "Route #", "route_long_name": "Route name"}).reset_index(drop=True)
    fastest_routes['Rank'] = fastest_routes['km/h'].rank(ascending=False).astype(int)

    top_routes = go.Figure(data=[go.Table(
                columnwidth = [10,45,10],
        header=dict(
            values=list(fastest_routes[['Rank', 'Route', 'km/h']]),
            line_color='darkslategray',
            fill_color='#005daa',
            align='center',
            font=dict(color='#FFFFFF', size=20, weight='bold'),
            height=40
        ),
        cells=dict(
            values=[fastest_routes['Rank'], fastest_routes['Route'], fastest_routes['km/h']],
            line_color='darkslategray',
            fill_color='#49a942',
            align='center',
            font=dict(color='#FFFFFF', size=16, weight=['bold', 'normal']),
            height=40
        )
    )])
    top_routes.update_layout(
        width=750,
        title='3 fastest routes (by average speed)',
        title_x=0.5,
        title_font=dict(size=25, weight='bold')
    )   
    save_plot(top_routes)
    
    # 3 slowest routes
    slowest_routes = routes_speeds[['km/h', 'route_short_name', 'route_long_name']].tail(3)
    slowest_routes['Route'] = slowest_routes['Route #'] + ' (' + slowest_routes['Route name'] + ')'
    slowest_routes['km/h'] = np.round(slowest_routes['km/h'], decimals=2)
    slowest_routes = slowest_routes.rename(columns={"route_short_name": "Route #", "route_long_name": "Route name"}).reset_index(drop=True)
    slowest_routes['Rank'] = slowest_routes['km/h'].rank(ascending=True).astype(int)
    slowest_routes = slowest_routes.sort_values('Rank')

    bottom_routes = go.Figure(data=[go.Table(
                columnwidth = [10,45,10],
        header=dict(
            values=list(slowest_routes[['Rank', 'Route', 'km/h']]),
            line_color='darkslategray',
            fill_color='#005daa',
            align='center',
            font=dict(color='#FFFFFF', size=20, weight='bold'),
            height=40
        ),
        cells=dict(
            values=[slowest_routes['Rank'], slowest_routes['Route'], slowest_routes['km/h']],
            line_color='darkslategray',
            fill_color='#49a942',
            align='center',
            font=dict(color='#FFFFFF', size=16, weight=['bold', 'normal']),
            height=40
        )
    )])
    bottom_routes.update_layout(
        width=750,
        title='3 slowest routes (by average speed)',
        title_x=0.5,
        title_font=dict(size=25, weight='bold')
    )
    save_plot(bottom_routes)
    
    
    
    # 3 most visited stops
    popular_stops = stop_frequency[['times_visited', 'stop_name', 'stop_code']].sort_values('times_visited', ascending=False)
    popular_stops = popular_stops.head(3).reset_index(drop=True)
    popular_stops['Rank'] = popular_stops['times_visited'].rank(ascending=False).astype(int)
    popular_stops['stop_code'] = popular_stops['stop_code'].astype('int64')
    popular_stops['Stop'] = popular_stops['stop_code'].astype('string') + ' (' + popular_stops['stop_name'] + ')'
    popular_stops = popular_stops.rename(columns={"times_visited": "# of visits"}).reset_index(drop=True)
    
    stops = go.Figure(data=[go.Table(
                columnwidth = [10,55,15],
        header=dict(
            values=list(popular_stops[['Rank', 'Stop', '# of visits']]),
            line_color='darkslategray',
            fill_color='#49a942',
            align='center',
            font=dict(color='#FFFFFF', size=20, weight='bold'),
            height=40
        ),
        cells=dict(
            values=[popular_stops['Rank'], popular_stops['Stop'], popular_stops['# of visits']],
            line_color='darkslategray',
            fill_color='#005daa',
            align='center',
            font=dict(color='#FFFFFF', size=16, weight=['bold', 'normal']),
            height=40
        )
    )])
    stops.update_layout(
        width=850,
        title='3 most visited stops (December 1st to 5th)',
        title_x=0.5,
        title_font=dict(size=25, weight='bold')
)
    save_plot(stops)  

    # Average number of stop visits compared to wheelchair accessibility
    stop_accessibility = pd.DataFrame(popular_stops.groupby('wheelchair_accessible')['times_visited'].mean()).reset_index()
    stop_accessibility['times_visited'] = np.round(stop_accessibility['times_visited'])
    stop_accessibility = px.bar(stop_accessibility, x='times_visited', y='wheelchair_accessible', color='wheelchair_accessible', 
                width=700, height=400, color_discrete_sequence=['#009ac7', '#f78e1e'],
                title='Average number of stop visits compared to wheelchair accessibility ', 
                            labels={'times_visited':'# of visits', 'wheelchair_accessible': 'Wheelchair accessibility'})
    stop_accessibility.update_yaxes(visible=False, showticklabels=False)
    stop_accessibility.show()
    save_plot(stop_accessibility) 
    
     # Percentage of wheelchair accessible stops
    wheelchair_plot = px.pie(stop_frequency, values='wheelchair_boarding', 
                            names='wheelchair_accessible', 
                            title='Percentage of wheelchair accessible stops', 
                            color_discrete_sequence=['#009ac7', '#f78e1e'],
                            labels={'1': 'Accessible', '2': 'Non-accessible'})
    save_plot(wheelchair_plot)
    
def save_plot(plot):
    plot.write_html(plot + ".html")
    plot.write_image(plot + ".png")
    
if __name__ == '__main__':
    main()



