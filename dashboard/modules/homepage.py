import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd

def homepage_wrapper(stats, page_size):
    types_list = []
    for single_type in stats['session_cards']:
        if stats['session_cards'][single_type]['count'] == 0:
            is_disabled = True
        elif single_type == 'All Sessions':
            is_disabled = False
        else:
            is_disabled = False

        types_list.append(
            html.Button(
                '{} ({})'.format(
                    single_type, 
                    stats['session_cards'][single_type]['count']
                    ),
            id=single_type,
            disabled = is_disabled
            )
        )

    pages_count = int(round(stats['choice_table'].shape[0] / page_size, 0))

    if stats['choice_table'].shape[0] < page_size:
        pages_count = 1
    else:
        if pages_count == 0:
            pages_count = 1
        else:
            pages_count += 1
            
    elements_list = html.Div(
        [
        html.Div(
                    html.H1(
                        'Choose Session'
                    ),
                    id='subtitle-wrapper'
                ),
        html.Div(
                    types_list,
                    id='types-wrapper'
                ),
        dash_table.DataTable(
        id='datatable-paging-page-count',
        columns=[{"name": i, "id": i, 'presentation': 'markdown'} if i == 'Team' else {"name": i, "id": i} for i in stats['choice_table'].columns],
        filter_query='',
        page_current=0,
        page_size=page_size,
        page_action='custom',
        page_count=pages_count,
        style_header={'border': '0 !important'},
        style_cell={'textAlign': 'left'},
        style_cell_conditional=[
            {
                'if': {'column_id': ''},
                'width': '20px'
            },
            {
                'if': {'column_id': 'Team'},
                'width': '222px'
            },
            {
                'if': {'column_id': 'Session Time'},
                'width': '250px'
            },
            {
                'if': {'column_id': 'Session Track'},
                'width': '167px'
            },
            {
                'if': {'column_id': 'Laps'},
                'width': '96px'
            },
            {
                'if': {'column_id': 'Session Duration'},
                'width': '133px'
            }
        ]
    )
        ],
        id='page-content',
        style={'height': '613px'}
    )
    
    return elements_list