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
#import modules.tiles as tiles

ext_styles = [
    dbc.themes.BOOTSTRAP,
    {
    'href': 'https://use.fontawesome.com/releases/v5.13.0/css/all.css',
    'rel': 'stylesheet',
    'integrity': 'sha384-50oBUHEmvpQ+1lW4y57PTFmhCaXp0ML5d60M1M7uH2+nqUivzIebhndOJK28anvf',
    'crossorigin': 'anonymous'
}
    ]

app = dash.Dash(external_stylesheets=ext_styles)
server = app.server

sidebar = navigation.navigation_bar()

tilebar = html.Div(
    style=css.CENTERED,
    id='page-content'
    )

app.layout = html.Div([dcc.Location(id="url"), sidebar, tilebar], style={'height': '100%'})

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname in ["/", "/homepage"]:
        # return html.Div(
        #     tiles.tile_bar(),
        #     style=css.TILE_WRAPPER
        # )
        return html.P("This is the content of homepage. Yay!")
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

@app.callback(Output("session-indicator", "children"), [Input("url", "pathname")])
def render_sidebar(pathname):
    if pathname not in ["/", "/homepage"]:
        message = html.P("Racing session: {}".format(''))
    else:
        message = html.P("Racing session: to be selected")
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
    if pathname == '/':
        indeks = 0
    else:
        if pathname is not None:
            # do not include homepage
            pathname_stripped = pathname[1:]
            indeks = list(sct.SECTIONS.keys()).index(pathname_stripped)
        else:
            indeks = 0
            pathname_stripped = 'homepage'
        output_list[indeks] = html.Img(
                src=sct.SECTIONS[pathname_stripped]['icon'],
                height="25px",
                style=css.GREEN_COLOR
                )
    return output_list

if __name__ == "__main__":
    app.title = 'F1 Telemetry App'
    app.run_server('0.0.0.0', port=5005)