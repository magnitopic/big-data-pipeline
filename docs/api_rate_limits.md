# API Rate Limits

## OpenSky
- Authenticated requests recommended.
- Rate limited (varies by endpoint).
- Avoid calling more than 1 request per 10 seconds per endpoint to prevent 429 errors.

## OpenWeather (Free Tier)
- 60 calls per minute.
- Only API version 2.5 is available on Free Tier.
- OneCall API 3.0 is NOT available → returns 401 Invalid API Key.
