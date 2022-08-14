import datazimmer as dz


class RealEstate(dz.AbstractEntity):
    property_id = int


class RealEstateRecord(dz.AbstractEntity):
    propety_id = dz.Index & RealEstate
