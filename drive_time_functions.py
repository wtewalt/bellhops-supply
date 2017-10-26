"""Functions used to compute round-trip distances and durations

Used in compute_drive_times.py
"""


import pandas as pd
import requests
import numpy as np


def make_url(origin, destination):
    """Create URLs from 2 lat/long pairs."""
    base_url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins="
    my_url = "{base_url}{origin}&destinations={destination}".format(
        base_url = base_url,
        origin = origin,
        destination = destination)
    return my_url


def get_distance_miles(url):
    """Use URL to get distance in miles."""

    response = requests.get(url)
    response_json = response.json()
    d_km = response_json['rows'][0]['elements'][0]['distance']['value']/1000
    #Conversion km --> miles constant
    miles_per_km = 0.621371
    d_miles = d_km * miles_per_km
    return d_miles


def get_trip_duration_hours(url):
    """Use URL to get distance in miles."""

    response = requests.get(url)
    response_json = response.json()
    duration_hours = response_json['rows'][0]['elements'][0]['duration']['value']/3600
    return duration_hours


def compute_round_trip(origin_address, first_stop, last_stop):
    """Compute the round trip distance based on 3 locations.

    Uses either addresses or lat_lon pairs in the form 'lat,lon'
    """

    # Create urls
    origin_to_first_location_url = make_url(origin_address, first_stop)
    first_to_last_location_url = make_url(first_stop, last_stop)
    last_location_to_origin_url = make_url(last_stop, origin_address)

    #Get Distances
    origin_to_first_location_dist = get_distance_miles(origin_to_first_location_url)
    first_to_last_location_dist = get_distance_miles(first_to_last_location_url)
    last_location_to_origin_dist = get_distance_miles(last_location_to_origin_url)
    #Calculate the round trip distance
    round_trip_dist = origin_to_first_location_dist + first_to_last_location_dist + last_location_to_origin_dist

    #Get Durations
    origin_to_first_location_dur = get_trip_duration_hours(origin_to_first_location_url)
    first_to_last_location_dur = get_trip_duration_hours(first_to_last_location_url)
    last_location_to_origin_dur = get_trip_duration_hours(last_location_to_origin_url)
    #Calculate the round trip duration in hours
    round_trip_duration = origin_to_first_location_dur + first_to_last_location_dur + last_location_to_origin_dur

    return round_trip_dist, round_trip_duration


def get_lat_lon(addresses):
    """Obtain lat/lon for all addresses in a list.

    Output is 2 lists.
    These are the lat and lon coordinates.
    """
    
    lats = []
    lons = []
    #lat_lon = []
    for address in addresses:
        response = requests.get(
            'https://maps.googleapis.com/maps/api/geocode/json?address={full_address}'.format(
            full_address = address
            )
        )
        #response to JSON
        resp_json_payload = response.json()
        # extract lat, lon values
        new_lat = resp_json_payload['results'][0]['geometry']['location']['lat']
        new_lon = resp_json_payload['results'][0]['geometry']['location']['lng']
        lats = np.append(lats, new_lat)
        lons = np.append(lons, new_lon)
        #lat_lon = np.append(lat_lon, str(new_lat) + ',' + str(new_lon))
    return lats, lons
