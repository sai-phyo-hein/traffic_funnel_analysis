import pandas as pd
import plotly.graph_objects as go

import plotly.express as px
from dash import Dash, dash_table
from dash import dcc
from dash import html
from dash.dependencies import Input, Output# Load Data

### Read Data
df = pd.read_parquet('simulated_data.parquet.gzip')

### labelling
label = list(set(df['from_'].unique().tolist() + df['to_'].unique().tolist()))
label_map = {}
for l in label:
    label_map[l] = label.index(l)
flow = df.groupby(['from_', 'to_'], as_index = False).agg(
    count = ('customer', 'count')
).replace(label_map)

### Create Dashboard
fig = go.Figure(data=[go.Sankey(
    node = dict(
      pad = 30,
      thickness = 30,
      line = dict(color = "black", width = 0.5),
      label = label,

    ),
    link = dict(
      source = flow['from_'],
      target = flow['to_'],
      value =  flow['count']
  ))])

app = Dash(__name__) # Build App
app.layout = html.Div(
    ### main div
    style = {"background-color": "white", "vertical-align":"top", "vertical-align": "Top"},
    children = [
        html.Div(
            ### title section div
            style = {"background-color": 'skyblue', "vertical-align":"top", "vertical-align": "Top"},
            children = [
                html.Div(
                    ### title div
                    style = {"background-color": 'skyblue', "display": "inline-block", "width": "50%", "height": "50px", "vertical-align": "Top"},
                    children = [
                        html.H2("Cutomer Journey Funnel Demo (Simulated Data)"), 
                    ]
                ),
                html.Div(
                    ### time filter div
                    style = {"background-color": 'skyblue', "display": "inline-block", "width": "50%", "height": "50px",},
                    children = [
                        html.P(" ", style = {"display": "inline-block", "width": "10%", }),
                        html.P("Start : ", style = {"display": "inline-block", "width": "10%",}),
                        dcc.Input(
                            id = 'start', type = 'time', value = '00:00', style = {"display": "inline-block", "width": "25%",}
                        ),

                        html.P(" ", style = {"display": "inline-block", "width": "10%",}),
                        html.P("End : ", style = {"display": "inline-block", "width": "10%",}),
                        dcc.Input(
                            id = 'end', type = 'time', value = '23:59', style = {"display": "inline-block", "width": "25%",}
                        )
                    ]

                ),
            ]

        ),
        html.Div(
            style = {"background-color": 'skyblue', "vertical-align": "Top"}, 
            children = [
                dcc.Dropdown(options = ['count','unique_count'], id = 'count_or_unique', value = 'count'), 
                html.P("Metric : Customer Count")
            ]
        ), 
        
        html.Div(
            ### Graph Section
            children = [
                dcc.Graph(id = 'flow_graph', figure = fig),
            ]
        ),
        html.Div(
            ### Detail Section
            children = [
                html.Div(
                    style = {"background-color": 'skyblue', "vertical-align": "Top"},
                    ### Subtitle Section
                    children = [
                        html.Div(
                            ### Subtitle div
                            style = {"background-color": 'skyblue', "display": "inline-block", "width": "25%", "vertical-align": "Top"},
                            children = [
                                html.H3("Details : ")
                            ]
                        ),
                        html.Div(
                            ### node options div
                            style = {"background-color": 'skyblue', "display": "inline-block", "width": "75%", "vertical-align": "Top"},
                            children = [
                                html.Div(
                                    style = {"background-color": 'skyblue', "display": "inline-block", "width": "50%", "vertical-align": "Top"},
                                    children = [
                                        html.Label([
                                            "From : ",
                                            dcc.Dropdown(id = 'from_dropdown', options= df.from_.unique().tolist(), value = 'DeepLink'),
                                        ]),
                                    ]
                                ),
                                html.Div(
                                    style = {"background-color": 'skyblue', "display": "inline-block", "width": "50%", "vertical-align": "Top"},
                                    children = [
                                        html.Label([
                                            "To : ",
                                            dcc.Dropdown(id = 'to_dropdown', options= df.to_.unique().tolist(), value = 'LandingPage'),
                                        ]),
                                    ]
                                ),


                                #html.P("To : ", style = {"display": "inline-block", "width": "10%"}),

                            ]
                        )
                    ]

                ),
                html.Div(
                    ### Table Sections
                    children = [
                        html.Div(
                            style = {"background-color": 'white', "vertical-align": "Top" },
                            children = [
                                html.H4("Path"),
                                html.Div(
                                    ### Path Performance
                                    children = [
                                        html.Div(
                                            style = {"background-color": 'grey', "vertical-align": "Top", "border": "1px black solid"},
                                            children = [
                                                html.H5("Overall") ,
                                                dash_table.DataTable(
                                                    df[(df.from_ == 'DeepLink') & (df.to_ == 'HomePage')].groupby(
                                                        ['from_', 'to_'], as_index = False
                                                    ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['from_', 'to_', 'customer_count']],
                                                    id = 'overall_path', page_size = 10
                                                ),
                                            ]
                                        ),
                                        html.Div(
                                            style = {"vertical-align": "top"},

                                            children = [
                                                html.Div(
                                                    style = {'background-color': 'grey', "display": "inline-block", "width" : "69.5%", "vertical-align": "Top" , "border": "1px black solid"},
                                                    children = [
                                                        html.H5("Path Flow : "),
                                                        dash_table.DataTable(
                                                            df[(df.from_ == 'DeepLink') & (df.to_ == 'HomePage')].groupby(
                                                                ['from_id', 'to_id'], as_index = False
                                                            ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['from_id', 'to_id', 'customer_count']],
                                                            id = 'path_flow', page_size = 10
                                                        ),
                                                    ]
                                                ),
                                                html.Div(
                                                    style = {'background-color': 'grey', "display": "inline-block", "width" : "30%" , "vertical-align": "Top", "border": "1px black solid"},
                                                    children = [
                                                        html.H5("Path Customer: ", style = {"display": "inline-block"}),
                                                        html.Button("Export", style = {"display": "inline-block"}),
                                                        dash_table.DataTable(
                                                            df[(df.from_ == 'DeepLink') & (df.to_ == 'HomePage')].groupby(
                                                                'customer', as_index = False
                                                            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['customer', 'entry_count']],
                                                            id = 'path_customers', page_size = 10
                                                        ),
                                                    ]
                                                ),
                                            ]
                                        ),
                                    ]
                                )


                            ]

                        ),
                        html.Div(
                            style = {"background-color": 'white', "vertical-align": "Top"},
                            children = [
                                html.H4("Node"),
                                html.Div(
                                    style = {"vertical-align": "Top"},
                                    children = [
                                        html.Div(
                                            style = {"background-color": 'grey', "display": "inline-block", "width": "24.5%", "vertical-align": "Top", "border": "1px black solid"},
                                            children = [
                                                html.H5("From Node : "),
                                                dash_table.DataTable(
                                                    df[(df.from_ == 'DeepLink')].groupby(
                                                        'from_id', as_index = False
                                                    ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['from_id', 'customer_count']],
                                                    id = 'from_node', page_size = 10
                                                ),
                                            ]
                                        ),
                                        html.Div(
                                            style = {"background-color": 'grey', "display": "inline-block", "width": "25%", "vertical-align": "Top", "border": "1px black solid"},
                                            children = [
                                                html.H5("To Node : "),
                                                dash_table.DataTable(
                                                    df[(df.to_ == 'HomePage')].groupby(
                                                        'to_id', as_index = False
                                                    ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['to_id', 'customer_count']],
                                                    id = 'to_node', page_size = 10
                                                ),
                                            ]
                                        ),
                                        html.Div(
                                            style = {"background-color": 'grey', "display": "inline-block", "width": "25%", "vertical-align": "Top", "border": "1px black solid"},
                                            children = [
                                                html.H5("From Node Customers : ", style = {"display": "inline-block"}),
                                                html.Button("Export", style = {"display": "inline-block"}),
                                                dash_table.DataTable(
                                                    df[(df.from_ == 'DeepLink')].groupby(
                                                        'customer', as_index = False
                                                    ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['customer', 'entry_count']],
                                                    id = 'from_node_customers', page_size = 10
                                                ),
                                            ]
                                        ),
                                        html.Div(
                                            style = {"background-color": 'grey', "display": "inline-block", "width": "24.5%", "vertical-align": "Top", "border": "1px black solid"},
                                            children = [
                                                html.H5("To Node Customers : ", style = {"display": "inline-block"}),
                                                html.Button("Export", style = {"display": "inline-block"}),
                                                dash_table.DataTable(
                                                    df[(df.to_ == 'HomePage')].groupby(
                                                        'customer', as_index = False
                                                    ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records'), [{"name": i, "id": i} for i in ['customer', 'entry_count']],
                                                    id = 'to_node_customers', page_size = 10
                                                ),
                                            ]
                                        ),
                                    ]
                                )
                            ]
                        )
                    ]
                )
            ]

        )

    ]
)



# Define callback to update graph
@app.callback(
    [
        Output('flow_graph', 'figure'),
        Output('overall_path', 'data'),
        Output('path_flow', 'data'),
        Output('path_customers', 'data'),
        Output('from_node', 'data'),
        Output('to_node', 'data'),
        Output('from_node_customers', 'data'),
        Output('to_node_customers', 'data'),
    ],
    [
        Input("start", "value"),
        Input("end", "value"),
        Input("from_dropdown", "value"),
        Input("to_dropdown", "value"), 
        Input("count_or_unique", 'value'), 
    ]
)

def update_figure(start, end, from_dropdown, to_dropdown, count_or_unique):
    if count_or_unique == 'count': 
        start = start + ":00"
        end = end + ":59"
        temp_df = df[(df.time_ >= start) & (df.time_ <= end)].groupby(['from_', 'to_'], as_index = False).agg(
            customer_count = ('customer', 'count')
        ).replace(label_map)
        print(temp_df.shape)

        fig = go.Figure(data=[go.Sankey(
            node = dict(
            pad = 30,
            thickness = 30,
            line = dict(color = "black", width = 0.5),
            label = label,

            ),
            link = dict(
            source = temp_df['from_'],
            target = temp_df['to_'],
            value =  temp_df['customer_count']
        ))])

        overall_path_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby(
                ['from_', 'to_'], as_index = False
            ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records')

        path_flow_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby(
            ['from_id', 'to_id'], as_index = False
        ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records')

        path_customer_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        from_node_detail_df = df[(df.from_ == from_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('from_id', as_index = False
            ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records')

        to_node_detail_df = df[(df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('to_id', as_index = False
            ).agg(customer_count = ('customer', 'count')).sort_values('customer_count', ascending = False).to_dict('records')

        from_node_customer_df =  df[(df.from_ == from_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        to_node_customer_df = df[(df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        return fig, overall_path_df, path_flow_df, path_customer_df, from_node_detail_df, to_node_detail_df, from_node_customer_df, to_node_customer_df
    

    elif count_or_unique == 'unique_count': 
        start = start + ":00"
        end = end + ":59"
        temp_df = df[(df.time_ >= start) & (df.time_ <= end)].groupby(['from_', 'to_'], as_index = False).agg(
            count = ('customer', 'nunique')
        ).replace(label_map)
        print(temp_df.shape)

        fig = go.Figure(data=[go.Sankey(
            node = dict(
            pad = 30,
            thickness = 30,
            line = dict(color = "black", width = 0.5),
            label = label,

            ),
            link = dict(
            source = temp_df['from_'],
            target = temp_df['to_'],
            value =  temp_df['count']
        ))])

        overall_path_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby(
                ['from_', 'to_'], as_index = False
            ).agg(customer_count = ('customer', 'nunique')).sort_values('customer_count', ascending = False).to_dict('records')

        path_flow_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby(
            ['from_id', 'to_id'], as_index = False
        ).agg(customer_count = ('customer', 'nunique')).sort_values('customer_count', ascending = False).to_dict('records')

        path_customer_df = df[(df.from_ == from_dropdown) & (df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        from_node_detail_df = df[(df.from_ == from_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('from_id', as_index = False
            ).agg(customer_count = ('customer', 'nunique')).sort_values('customer_count', ascending = False).to_dict('records')

        to_node_detail_df = df[(df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('to_id', as_index = False
            ).agg(customer_count = ('customer', 'nunique')).sort_values('customer_count', ascending = False).to_dict('records')

        from_node_customer_df =  df[(df.from_ == from_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        to_node_customer_df = df[(df.to_ == to_dropdown) & (df.time_ >= start) & (df.time_ <= end)].groupby('customer', as_index = False
            ).agg(entry_count = ('time_', 'count')).sort_values('entry_count', ascending = False).to_dict('records')

        return fig, overall_path_df, path_flow_df, path_customer_df, from_node_detail_df, to_node_detail_df, from_node_customer_df, to_node_customer_df
    


app.run_server()