import datetime as dt

import datazimmer as dz


class Seller(dz.AbstractEntity):
    id = dz.Index & int
    name = str
    website_url = str
    hide_contact_form = float
    realtors_prohibited = float
    agency = str


class Location(dz.AbstractEntity):
    id = dz.Index & int


class RealEstate(dz.AbstractEntity):
    id = dz.Index & int
    cluster_id = int
    offer_type = str
    area_size = float
    attic_type = str
    available_from = str
    balcony_size = float
    bathroom_toilet_separation = str
    building_floor_count = str
    comfort_level = str
    condition = str
    energy_efficiency_rating = str
    without_gas_connection = str
    energy_efficient = str
    is_deleted = str
    floor = str
    furnishment = str
    has_air_conditioner = float
    has_barrier_free_access = float
    has_elevator = float
    has_equipments = float
    has_garden_access = float
    inner_height = str
    is_pets_allowed = float
    is_smoking_allowed = float
    location_id = Location
    orientation = str
    participated_in_the_panel_program = float
    solar_panel = str
    room_count = float
    small_room_count = float
    subtype = str
    type = str
    common_charges = str
    average_gas_consumption = str
    average_electric_consumption = str
    insulation = str
    insulation_thickness = str
    view = str
    year_of_construction = float
    minimum_rental_period_month = float
    is_unincorporated_area = float
    photo_count = float
    description = str
    reference_id = str
    garden_size = float
    is_bank_claim_offer = float
    is_rental_right_offer = float
    is_outdated = float
    is_active = float
    updated_at = str
    seller_id = Seller
    lot_size = float
    has_basement = float
    tender_document = str
    operational_cost = str


class Contact(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    phone_number = dz.Index & str


class Price(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    currency = dz.Index & str
    amount = int
    interval_y = float
    interval_m = float
    interval_d = float


class RealEstateRecord(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    recorded = dz.Index & dt.datetime
    photo_count = int
    price = str
    address = str
    area_size = str
    room_count = str
    balcony_size = str


class UtilityCost(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    currency = str
    amount = int
    interval_y = float
    interval_m = float
    interval_d = float


class Heating(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    heating_type = dz.Index & str


class Parking(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    type = str
    condition = str
    amount = float
    currency = str
    interval_y = float
    interval_m = float
    interval_d = float


class Label(dz.AbstractEntity):
    property_id = dz.Index & RealEstate
    label = dz.Index & str
