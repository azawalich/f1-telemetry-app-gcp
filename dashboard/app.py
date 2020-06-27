import dash
import dash_bootstrap_components as dbc

ext_styles = [dbc.themes.BOOTSTRAP]

OG_META_TAGS = [
        {
            'property': 'og:title',
            'content': 'F1 Telemetry App - Aleksander Zawalich'
        },
        {
            'property': 'og:description',
            'content': 'F1 telemetry app hosted on GCP'
        },
        {
            'property': 'og:image',
            'content': 'https://www.codemasters.com/wp-content/uploads/2019/03/f1_2019_monza_010.jpg'
        },
        {
            'property': 'og:url',
            'content': 'http://f1.zawalich.pl'
        },
        {
            'property': 'og:type',
            'content': 'website'
        },
        {
            'http-equiv': 'X-UA-Compatible',
            'content': 'IE=edge'
        },
        {
            'charset': 'UTF-8'
        },
    ]

app = dash.Dash(
    external_stylesheets=ext_styles,
    suppress_callback_exceptions=True,
    meta_tags=OG_META_TAGS
    )

server = app.server