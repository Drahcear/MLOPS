from datetime import datetime
import json
from kafka import KafkaProducer
import random

Country = ['United States','Afghanistan','Albania','Algeria','American Samoa','Andorra','Angola','Anguilla','Antarctica','Antigua And Barbuda','Argentina','Armenia','Aruba','Australia','Austria','Azerbaijan','Bahamas','Bahrain','Bangladesh','Barbados','Belarus','Belgium','Belize','Benin','Bermuda','Bhutan','Bolivia','Bosnia And Herzegowina','Botswana','Bouvet Island','Brazil','Brunei Darussalam','Bulgaria','Burkina Faso','Burundi','Cambodia','Cameroon','Canada','Cape Verde','Cayman Islands','Central African Rep','Chad','Chile','China','Christmas Island','Cocos Islands','Colombia','Comoros','Congo','Cook Islands','Costa Rica','Cote D`ivoire','Croatia','Cuba','Cyprus','Czech Republic','Denmark','Djibouti','Dominica','Dominican Republic','East Timor','Ecuador','Egypt','El Salvador','Equatorial Guinea','Eritrea','Estonia','Ethiopia','Falkland Islands (Malvinas','Faroe Islands','Fiji','Finland','France','French Guiana','French Polynesia','French S. Territories','Gabon','Gambia','Georgia','Germany','Ghana','Gibraltar','Greece','Greenland','Grenada','Guadeloupe','Guam','Guatemala','Guinea','Guinea-bissau','Guyana','Haiti','Honduras','Hong Kong','Hungary','Iceland','India','Indonesia','Iran','Iraq','Ireland','Israel','Italy','Jamaica','Japan','Jordan','Kazakhstan','Kenya','Kiribati','North Korea','South Korea','Kuwait','Kyrgyzstan','Laos','Latvia','Lebanon','Lesotho','Liberia','Libya','Liechtenstein','Lithuania','Luxembourg','Macau','Macedonia','Madagascar','Malawi','Malaysia','Maldives','Mali','Malta','Marshall Islands','Martinique','Mauritania','Mauritius','Mayotte','Mexico','Micronesia','Moldova','Monaco','Mongolia','Montserrat','Morocco','Mozambique','Myanmar','Namibia','Nauru','Nepal','Netherlands','Netherlands Antilles','New Caledonia','New Zealand','Nicaragua','Niger','Nigeria','Niue','Norfolk Island','Northern Mariana Islands','Norway','Oman','Pakistan','Palau','Panama','Papua New Guinea','Paraguay','Peru','Philippines','Pitcairn','Poland','Portugal','Puerto Rico','Qatar','Reunion','Romania','Russian Federation','Rwanda','Saint Kitts And Nevis','Saint Lucia','St Vincent/Grenadines','Samoa','San Marino','Sao Tome','Saudi Arabia','Senegal','Seychelles','Sierra Leone','Singapore','Slovakia','Slovenia','Solomon Islands','Somalia','South Africa','Spain','Sri Lanka','St. Helena','St.Pierre','Sudan','Suriname','Swaziland','Sweden','Switzerland','Syrian Arab Republic','Taiwan','Tajikistan','Tanzania','Thailand','Togo','Tokelau','Tonga','Trinidad And Tobago','Tunisia','Turkey','Turkmenistan','Tuvalu','Uganda','Ukraine','United Arab Emirates','United Kingdom','Uruguay','Uzbekistan','Vanuatu','Vatican City State','Venezuela','Viet Nam','Virgin Islands (British)','Virgin Islands (U.S.)','Western Sahara','Yemen','Yugoslavia','Zaire','Zambia','Zimbabwe']



def get_country(countries):
    return random.choice(countries)
class Card:
    def __init__(self, name, number, CVC, expire, country = None):
        self.name = name
        self.number = number
        self.CVC = CVC
        self.expire = expire
        if country:
            self.country = country
        else:
            self.country = get_country(Country)
class Sender:
    def __init__(self, UID, topic, server, card):
        self.ID = str(UID)
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=server)        
        self.card = card        
        
    def getDate(self):
        now = datetime.now()
        # dd/mm/YY H:M:S
        return now.strftime("%Y/%m/%d %H:%M:%S")
        
    def makeMessage(self):
        return {"date": self.getDate(), "ID" : str(self.ID), "country": self.card.country,\
                "name": self.card.name, "number": self.card.number, "CVC": self.card.CVC, "expire": self.card.expire}
        
    def send(self):        
        msg = self.makeMessage()
        msg = json.dumps(msg)
        print(msg)
        self.producer.send(self.topic, msg.encode('utf-8'))
        self.producer.flush()