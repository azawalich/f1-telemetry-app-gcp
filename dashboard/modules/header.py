import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import data_assets.style as css
import data_assets.sections as sct

navigation_elements = []

for single_section in sct.SECTIONS.keys():
    # jak time trial, to nie pokazuj bezsensownych linków, np drivers ranking
    temp_section = sct.SECTIONS[single_section]
    element = dbc.Row(
        [   
            html.Div(
            className="lead",
            id="{}-current-indicator".format(single_section)
                ),
            dbc.NavLink(
                temp_section['name'],
                href="/{}".format(single_section),
                id=single_section,
                style=temp_section['img_style'],
                disabled=temp_section['disabled'])
        ], align="center", no_gutters=True
    )
    navigation_elements.append(element)

def global_stats_elements(stats):
    stats_list = []
    for single_stat in stats:
        if list(stats.keys()).index(single_stat) < 3:
            element = html.H2(stats[single_stat])
        else:
            element = html.H3(stats[single_stat])

        stats_list.append(
            html.Div(
                    [
                        html.P(single_stat.replace('_', ' ').title()),
                        element
                    ],
                    className='global-stats'
                )
        )

    return stats_list

def header_bar(stats = None):
    headbar = [
        html.Div(
            [
                html.H1(
                    [
                        'Global',
                        html.Span(
                            'Statistics',
                            style = {
                                'display': 'block'
                                }
                            )
                    ]
            ),
            html.Div(
                id="session-indicator"
            ),
        ],
            id='title-wrapper'
        ), 
        html.Div(
            global_stats_elements(stats),
            id='global-stats-wrapper'
        )
    ]
    
    return headbar
