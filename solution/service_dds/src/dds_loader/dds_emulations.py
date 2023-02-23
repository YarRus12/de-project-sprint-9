data = {
            "object_id": 322519,
            "object_type": "order",
            "payload": {
                "id": 322519,
                "date": "2022-11-19 16:06:36",
                "cost": 300,
                "payment": 300,
                "status": "CLOSED",
                "restaurant": {
                    "id": "626a81cfefa404208fe9abae",
                    "name": "Кофейня №1"
                },
                "user": {
                    "id": "626a81ce9a8cd1920641e296",
                    "name": "Котова Ольга Вениаминовна"
                },
                "products": [
                    {
                        "id": "6276e8cd0cf48b4cded00878",
                        "price": 180,
                        "quantity": 1,
                        "name": "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ",
                        "category": "Выпечка"
                    },
                    {
                        "id": "6276e8cd0cf48b4cded0086c",
                        "price": 60,
                        "quantity": 2,
                        "name": "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ",
                        "category": "Закуски"
                    }
                ]
            }
        }


class ToDdsKafkaProducer():
   def produce(): 
       print("Message produced")
       return data #можно переписать на yeild 
    

class FromDdsKafkaProducer():
    def __init__(self) -> None:
        self.list_of_data = []
    
    def send(self, data): 
       self.list_of_data.append(data)
       print("Message received")
       
