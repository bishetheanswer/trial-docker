#!/bin/bash

prefect agent local start --key $(cat ~/prefect_service_account) --no-hostname-label