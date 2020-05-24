import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import os, sys
sys.path += [os.path.abspath(os.path.join('assets')), os.path.abspath(os.path.join('modules'))]

import assets.css_classes as css
import assets.sections as sct
import modules.navigation as navigation
import modules.homepage as homepage

ext_styles = [dbc.themes.BOOTSTRAP]

app = dash.Dash(
    external_stylesheets=ext_styles,
    suppress_callback_exceptions=True
    )
server = app.server

sidebar = navigation.navigation_bar()

tilebar = html.Div(
    style=css.CENTERED,
    id='page-content'
    )

def serve_layout():
    layout = html.Div([dcc.Location(id="url"), sidebar, tilebar], style={'height': '100%'})
    return layout

app.layout = serve_layout

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname in ["/", "/homepage"]:
        return homepage.tile_bar()
    elif pathname == "/session-summary":
        return html.P("This is the content of page 2. Yay!")
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )

@app.callback(Output("session-indicator", "children"), [Input('session-dropdown', 'value')])
def render_sidebar(value):
    output_session = value
    if isinstance(output_session, dict):
        output_session = value['value']
    message = html.B("Racing session: {}".format(output_session))
    return message

@app.callback(Output("cta-prompt", "children"), [Input('session-dropdown', 'value')])
def render_cta_prompt(value):
    # for first, direct entry
    message = None
    
    if isinstance(value, dict) == False:
        message = html.H1(
                'You are all set - now explore data by choosing sections in main menu!', 
                style=css.SECTION_HEADINGS
            )
    return message

@app.callback([Output(f"{i}-current-indicator", "children") for i in sct.SECTIONS.keys()], [Input("url", "pathname")])
def render_current(pathname):
    output_list = []
    for single_section in sct.SECTIONS.keys():
        output_list.append(
            html.Img(
                src=sct.SECTIONS[single_section]['icon'],
                height="25px",
                style=sct.SECTIONS[single_section]['img_style']
                )
        )

    indeks = 0
    pathname_stripped = 'homepage'

    if pathname not in [None, '/']:
        # do not include homepage
        pathname_stripped = pathname[1:]
        indeks = list(sct.SECTIONS.keys()).index(pathname_stripped)
    
    output_list[indeks] = html.Img(
            src=sct.SECTIONS[pathname_stripped]['icon'],
            height="25px",
            style=css.GREEN_COLOR
            )
    return output_list

if __name__ == "__main__":
    app.title = 'F1 Telemetry App'
    app.run_server('0.0.0.0')#, port=80)