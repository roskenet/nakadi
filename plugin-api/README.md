# Nakadi-plugin-api

Nakadi-plugin-api is an API to develop extensions for [Nakadi](https://github.bus.zalan.do/aruha/nakadi).

## Build

To build plugin-api, run `gradle jar`.

## Current plugin stubs

### ApplicationService

Plugins implementing ApplicationService are used to validate the `owning_application` field in an event type definition.

### AuthorizationService

Plugins implementing AuthorizationService are used to enforce per-event type and per-subscription authorization.
