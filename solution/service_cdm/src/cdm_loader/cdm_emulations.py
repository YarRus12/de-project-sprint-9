data_to_cdm = [
    {'id': '6276e8cd0cf48b4cded00878',
        'price': 180, 'quantity': 1, 
        'name': 'РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ', 
        'category': 'Выпечка',
        'user_id': '626a81ce9a8cd1920641e296'
    }, 
    {'id': '6276e8cd0cf48b4cded0086c', 
        'price': 60,
        'quantity': 2,
        'name': 'ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ',
        'category': 'Закуски',
        'user_id': '626a81ce9a8cd1920641e296'
    }
    ]


class ToCdmKafkaProducer():
   def produce(): 
       print("Message produced")
       return data_to_cdm #можно переписать на yeild 
   