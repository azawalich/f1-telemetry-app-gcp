import dash
import dash_core_components as dcc
import dash_html_components as html

from app import app
import callbacks

sidebar = html.Div(
    id='sidebar'
    )

content = html.Div(
    id='page-content-wrapper'
    )

header = html.Div(
    id='header-wrapper'
)

def serve_layout():
    layout = html.Div(
        html.Div(
        [
            dcc.Location(id="url"),
            sidebar,
            header,
            content,
            html.Div(0, id='paging-state-default', style={'display': 'none'}),
            html.Div('All Sessions', id='choice-default', style={'display': 'none'}),
            html.Div(id='paging-state', style={'display': 'none'}),
            html.Div(id='choice', style={'display': 'none'})
        ],
        id = 'layout-design',
        className = 'layout-design'
        ),
        id = 'scaleable-wrapper',
        className = 'scaleable-wrapper'
    )
    
    return layout

app.layout = serve_layout

if __name__ == "__main__":
    app.title = 'F1 Telemetry App - Aleksander Zawalich'
    app.run_server('0.0.0.0', port=5005)