import data_assets.style as css

SECTIONS = {
    'homepage': {
        'icon': 'assets/images/menu_arrow.svg',
        'name': 'Back'
    },
    'session-summary': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/table.svg',
        'name': 'Session Summary',
        'disabled': True,
        'table_cell_widths': [
                {
                    'if': {'column_id': ''},
                    'width': '40px'
                },
                {
                    'if': {'column_id': 'Name'},
                    'width': '65px'
                },
                {
                    'if': {'column_id': 'Nat.'},
                    'width': '65px'
                },
                {
                    'if': {'column_id': 'Team'},
                    'width': '252px'
                },
                {
                    'if': {'column_id': 'Laps'},
                    'width': '50px'
                },
                {
                    'if': {'column_id': 'Tot. Penalty'},
                    'width': '85px'
                },
                {
                    'if': {'column_id': 'Fastest Lap'},
                    'width': '100px'
                },
                {
                    'if': {'column_id': 'Gap'},
                    'width': '85px'
                },
                {
                    'if': {'column_id': 'Pits'},
                    'width': '45px'
                }
            ],
        'table_race_cell_widths': [
                {
                    'if': {'column_id': ''},
                    'width': '40px'
                },
                {
                    'if': {'column_id': 'Name'},
                    'width': '65px'
                },
                {
                    'if': {'column_id': 'Nat.'},
                    'width': '65px'
                },
                {
                    'if': {'column_id': 'Team'},
                    'width': '202px'
                },
                {
                    'if': {'column_id': 'Laps'},
                    'width': '50px'
                },
                {
                    'if': {'column_id': 'Time'},
                    'width': '120px'
                },
                {
                    'if': {'column_id': 'Int.'},
                    'width': '90px'
                },
                {
                    'if': {'column_id': 'Tot. Penalty'},
                    'width': '85px'
                },
                {
                    'if': {'column_id': 'Fastest Lap'},
                    'width': '90px'
                },
                {
                    'if': {'column_id': 'Gap'},
                    'width': '85px'
                },
                {
                    'if': {'column_id': 'Pits'},
                    'width': '35px'
                }
            ]
    },
    'session-laps': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/th.svg',
        'name': 'Session Laps',
        'table_cell_widths': [
            {
                'if': {'column_id': ''},
                'width': '40px'
            },
            {
                'if': {'column_id': 'Ach.'},
                'width': '50px'
            },
            {
                'if': {'column_id': 'Lap'},
                'width': '50px'
            },
            {
                'if': {'column_id': 'Name'},
                'width': '65px'
            },
            {
                'if': {'column_id': 'Nat.'},
                'width': '65px'
            },
            {
                'if': {'column_id': 'Team'},
                'width': '252px'
            },
            {
                'if': {'column_id': 'Lap Time'},
                'width': '100px'
            },
            {
                'if': {'column_id': 'Sector 1'},
                'width': '100px'
            },
            {
                'if': {'column_id': 'Sector 2'},
                'width': '100px'
            },
            {
                'if': {'column_id': 'Sector 3'},
                'width': '100px'
            },
            {
                'if': {'column_id': 'Gap'},
                'width': '85px'
            },
            {
                'if': {'column_id': 'Lap Invalid'},
                'width': '95px'
            },
            {
                'if': {'column_id': 'lap_diff'},
                'display': 'none'
            },
            {
                'if': {'column_id': 's1_diff'},
                'display': 'none'
            },
            {
                'if': {'column_id': 's2_diff'},
                'display': 'none'
            },
            {
                'if': {'column_id': 's3_diff'},
                'display': 'none'
            }
        ],
        'table_data_conditional': [
                {
                    'if': {
                        'filter_query': '{lap_diff} < 0',
                        'column_id': 'Lap Time'
                    },
                    'color': '#009C38'
                },
                {
                    'if': {
                        'filter_query': '{s1_diff} < 0',
                        'column_id': 'Sector 1'
                    },
                    'color': '#009C38'
                },
                {
                    'if': {
                        'filter_query': '{s2_diff} < 0',
                        'column_id': 'Sector 2'
                    },
                    'color': '#009C38'
                },
                {
                    'if': {
                        'filter_query': '{s3_diff} < 0',
                        'column_id': 'Sector 3'
                    },
                    'color': '#009C38'
                },
                {
                    'if': {
                        'filter_query': '{lap_diff} = 10',
                        'column_id': 'Lap Time'
                    },
                    'color': '#831E91'
                },
                {
                    'if': {
                        'filter_query': '{s1_diff} = 10',
                        'column_id': 'Sector 1'
                    },
                    'color': '#831E91'
                },
                {
                    'if': {
                        'filter_query': '{s2_diff} = 10',
                        'column_id': 'Sector 2'
                    },
                    'color': '#831E91'
                },
                {
                    'if': {
                        'filter_query': '{s3_diff} = 10',
                        'column_id': 'Sector 3'
                    },
                    'color': '#831E91'
                }
            ]
    },
    'driver-ranking': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/random.svg',
        'name': 'Driver Ranking'
    },
    'session-map': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/route.svg',
        'name': 'Session Map'
    },
    'telemetry-data': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/chart-line.svg',
        'name': 'Telemetry Data'
    },
    'tires-age': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/circle-notch.svg',
        'name': 'Tires Age'
    },
    'pure-pitwall': {
        'icon': 'https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/chart-pie.svg',
        'name': 'Pure Pitwall'
    }
}