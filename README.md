# aircraft-monitoring
Python project made for fun, useful for spotters you can either choose between automatic geolocation or you can type the address yourself.

API's Used: OpenSky Network REST API

(PS: All commits description are chosen by meta ai or whatever AI github uses because i lowkey forgot)

## Optional Auth

OpenSky now uses OAuth client credentials for authenticated REST access. Set both environment variables if you want better limits:

```powershell
$env:OPENSKY_CLIENT_ID="your_client_id"
$env:OPENSKY_CLIENT_SECRET="your_client_secret"
python nearby_aircraft_bot.py --radius-km 40
```

You can also pass `--client-id` and `--client-secret` directly.

# Radar Config

The config wizard lets you save:

- radar range in km
- polling interval
- HTTP timeout
- maximum number of aircraft rows to print
- whether to include aircraft on the ground
- optional minimum altitude filter
- optional maximum data age filter
- whether to request extended OpenSky state vectors
- close-aircraft alerts
- close-alert distance in km
- close-alert cooldown in seconds
- optional terminal bell on close alerts
