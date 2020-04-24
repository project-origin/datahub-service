# """
# Serves the app locally using Waitress for DEVELOPMENT PURPOSE ONLY.
# Uses port 8080 unless another port is provided as argument.
# """
# import argparse
# import waitress
#
# from datahub import app
# from datahub.settings import PROJECT_NAME
#
#
# # -- Arguments ---------------------------------------------------------------
#
# description = (
#     'Serves %s development server locally using Waitress for DEVELOPMENT '
#     'PURPOSE ONLY. Uses http://0.0.0.0:8080 unless otherwise specified.'
# ) % PROJECT_NAME
#
# parser = argparse.ArgumentParser(description=description)
# parser.add_argument('--host', type=str, default='0.0.0.0', help='Hostname or IP-address')
# parser.add_argument('--port', type=int, default=8080, help='Port')
# args = parser.parse_args()
#
#
# # -- Serve -------------------------------------------------------------------
#
# print('-' * 80)
# print('Running %s development server' % PROJECT_NAME)
# print('Do not use this for production')
# print('-' * 80)
# print('Serving on http://%s:%s' % (args.host, args.port), flush=True)
#
# waitress.serve(app, host=args.host, port=args.port)
