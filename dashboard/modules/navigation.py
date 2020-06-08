import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import assets.css_classes as css
import assets.sections as sct

navigation_elements = []

for single_section in sct.SECTIONS.keys():
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


def navigation_bar():
    navbar = html.Div(
    [
        dbc.Row(
            [
                html.Img(
                    src="https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg",
                    height="25px",
                    style=css.ROW_MARGIN_BIG), 
                html.H3("Telemetry App")
            ], align = 'left'
        ),
        html.Hr(),
        html.Div(
            className="lead",
            id="session-indicator"
        ),
        html.Hr(),
        dbc.Nav(
            navigation_elements,
            vertical=True
        ),
        html.Hr(),
        dcc.Markdown('''
        Powered with [F1 2019](https://www.codemasters.com/game/f1-2019/) data 
        by: [GCP](https://cloud.google.com) and [Dash](https://plotly.com/dash/)
        '''),
        dbc.Row(
            [
                dcc.Markdown('''
                Code repository available on [GitLab]
                (https://gitlab.com/azawalich/f1-telemetry-app-gcp/-/tree/master/dashboard) &nbsp;
                '''),
        html.Img(
            src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/brands/gitlab.svg",
            height="20px",
            style=css.GITLAB_COLOR)
            ],
            align='left',
            no_gutters=True
        ),
        
        dcc.Markdown('''
        For feedback and inquiries, [let's get in touch ;)](mailto:aleksander@zawalich.pl)
        '''),
        dcc.Markdown('''
        Dashboard code by [Aleksander Zawalich](http://zawalich.pl)
        '''),
        html.Hr(),
        html.I('All trademarks are property of their respective owners', className="small")
        
    ],
    style=css.SIDEBAR_STYLE,
    )
    return navbar