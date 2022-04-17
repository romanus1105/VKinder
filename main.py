from random import randrange
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
import sqlite3
import random

class VKinder():
    def __init__(self, c_token, u_token, db_file_path):
        self.c_token = c_token
        self.u_token = u_token
        self.c_vk = vk_api.VkApi(token=self.c_token)
        self.u_vk = vk_api.VkApi(token=self.u_token)
        self.longpoll = VkLongPoll(self.c_vk)
        self.c_session_api = self.c_vk.get_api()
        self.u_session_api = self.u_vk.get_api()
        self.db_file_path = db_file_path
        self.db_connection = sqlite3.connect(self.db_file_path)
        with self.db_connection:
            self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS bot_users ( 
                vk_id INTEGER PRIMARY KEY NOT NULL
            )
            ''')
            self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS candidates (
                vk_id INTEGER PRIMARY KEY NOT NULL,
                sex INTEGER NOT NULL,
                city INTEGER NOT NULL DEFAULT 1,
                byear TEXT NOT NULL
            )
            ''')
            self.db_connection.execute('''
            PRAGMA foreign_keys = ON
            ''')
            self.db_connection.execute('''
            CREATE TABLE IF NOT EXISTS seen (
                bot_user_vk_id INTEGER NOT NULL REFERENCES bot_users(vk_id),
                candidate_vk_id INTEGER NOT NULL REFERENCES candidates(vk_id)
            )
            ''')           

    def write_msg(self, user_id, message):
        self.c_vk.method('messages.send', {'user_id': user_id, 'message': message,  'random_id': randrange(10 ** 7),})

    def get_user_info(self, user_id):
        user_info = self.c_session_api.users.get(user_ids = user_id, fields = ['sex', 'bdate', 'city'])
        if 'bdate' in list(user_info[0].keys()):
            bday_user_list = user_info[0]['bdate'].split('.')
            if len(bday_user_list) == 3:
                user_birth_year = bday_user_list[-1]
            else:
                user_birth_year = None
        else:
            user_birth_year = None
        if 'city' in  list(user_info[0].keys()):
            user_city = user_info[0]['city']
        else:
            user_city = 0
        user_info = {
            'vk_id': user_id,
            'first_name': user_info[0]['first_name'],
            'last_name': user_info[0]['last_name'],
            'sex': user_info[0]['sex'],
            'birth_year': user_birth_year,
            'city': user_city
        }
        if user_info['birth_year'] == None:
            self.write_msg(user_id=user_id, message='В вашей анкете не указан год рождения. Напишите год вашего рождения:')
            for _event in self.longpoll.listen():
                if _event.type == VkEventType.MESSAGE_NEW:
                    if _event.to_me:
                        user_info['birth_year'] = _event.text
                        break
        
        with self.db_connection:
            cursor = self.db_connection.cursor()
            cursor.execute(f'SELECT * FROM bot_users WHERE vk_id = {user_info["vk_id"]}')
            is_present = bool(cursor.fetchone())
            if not is_present:
                self.db_connection.execute(f'''
                INSERT INTO bot_users (vk_id) VALUES ({user_info['vk_id']})
                ''')
        return user_info

    def find_soulmate_candidates(self, for_whom_sex, for_whom_byear, for_whom_city):
        search_list = self.u_session_api.users.search(
            q = '', 
            sort = 0, 
            count = 1000, 
            city = for_whom_city, 
            birth_year = for_whom_byear, 
            sex = [1 if for_whom_sex == 2 else 2],
            fields = ['domain', 'sex', 'city'],
            has_photo = 1,
            status = 6
            )
        for item in search_list['items']:
            if not item['can_access_closed']:
                continue
            to_insert = {
                'vk_id': item['id'],
                'sex': item['sex'],
                'city': None,
                'byear': for_whom_byear
            }
            try:
                to_insert['city'] = item['city']['id']
            except:
                to_insert['city'] = '0'
            with self.db_connection:
                cursor = self.db_connection.cursor()
                cursor.execute(f'SELECT * FROM candidates WHERE vk_id = {to_insert["vk_id"]}')
                is_present = bool(cursor.fetchone())
                if not is_present:
                    self.db_connection.execute(f'''
                    INSERT INTO candidates (vk_id, sex, city, byear)
                    VALUES ({to_insert['vk_id']},{to_insert['sex']},{to_insert['city']},{to_insert['byear']})
                    ''')

    def get_candidate(self, for_whom_id, for_whom_sex, for_whom_byear, for_whom_city):
        if for_whom_city == 0:
            select_query = f'''
            SELECT vk_id FROM candidates
            WHERE sex = {1 if for_whom_sex == 2 else 2} AND
            byear = {for_whom_byear} 
            '''
        else:
            select_query = f'''
            SELECT vk_id FROM candidates
            WHERE sex = {1 if for_whom_sex == 2 else 2} AND
            byear = {for_whom_byear} AND
            city = {for_whom_city}
            '''
        with self.db_connection:
            cursor = self.db_connection.cursor()
            cursor.execute(select_query)          
            result = cursor.fetchall()
        with self.db_connection:
            cursor = self.db_connection.cursor()
            cursor.execute(f'''
            SELECT candidate_vk_id FROM seen WHERE bot_user_vk_id = {for_whom_id}
            ''')
            seen_list = cursor.fetchall()
        for item in result:
            if item in seen_list:
                result.remove(item)
        any_1_candidates = []
        for item in random.sample(result, 1):
            item = item[0]
            any_1_candidates.append(item)
            with self.db_connection:
                cursor = self.db_connection.cursor()
                cursor.execute(f'SELECT * FROM seen WHERE bot_user_vk_id = {for_whom_id} AND candidate_vk_id = {item}')
                is_present = bool(cursor.fetchone())
                if not is_present:
                    self.db_connection.execute(f'''
                    INSERT INTO seen (bot_user_vk_id, candidate_vk_id)
                    VALUES ({for_whom_id}, {item})
                    ''')
        return any_1_candidates
    
    def offer_candidates(self, for_whom_id, candidates_list):
        for candidate in candidates_list:
            item_user_info = self.c_session_api.users.get(
                user_id = candidate, 
                fields = ['bdate', 'city', 'sex', 'relation', 'domain']
                )
            photos = self.u_session_api.photos.get(
                owner_id = item_user_info[0]['id'],
                album_id = 'profile',
                extended = 1 
            )
            url_and_likes_dict = {}
            for photo in photos['items']:
                url_and_likes_dict[photo['sizes'][-1]['url']] = photo['likes']['count']
            sorted_tuple = sorted(url_and_likes_dict.items(), key=lambda x: x[0])
            up_to_3_top_photos = []
            try:
                up_to_3_top_photos.append(sorted_tuple[-1])
                up_to_3_top_photos.append(sorted_tuple[-2])
                up_to_3_top_photos.append(sorted_tuple[-3])
            except:
                pass 
            message = f'''
            {item_user_info[0]['first_name']} {item_user_info[0]['last_name']}
            https://vk.com/{item_user_info[0]['domain']}
            '''
            for photo in up_to_3_top_photos:
                message = message + f'\n{photo[0]}'
            self.write_msg(user_id=for_whom_id, message = message)

def main():
    с_token = ''
    u_token = ''
    db_file_path = 'vk_bot_db.db'
    vkinder = VKinder(c_token = с_token, u_token=u_token, db_file_path = db_file_path)

    for event in vkinder.longpoll.listen():
        if event.type == VkEventType.MESSAGE_NEW:
            if event.to_me:
                text = event.text
                bot_user_id = event.user_id
                user_info = vkinder.get_user_info(user_id=bot_user_id)
                vkinder.write_msg(user_id = bot_user_id, message = '''
                Напишите любое сообщение, чат-бот будет выдавать Вам подходящий вариант.
                Для выхода напишите Q.
                ''')
                for _event in vkinder.longpoll.listen():
                    if _event.type == VkEventType.MESSAGE_NEW:
                        if _event.to_me:
                            if _event.text != 'Q':
                                vkinder.find_soulmate_candidates(for_whom_sex = user_info['sex'], for_whom_byear = user_info['birth_year'], for_whom_city = user_info['city'])
                                any_3_candidates = vkinder.get_candidate(for_whom_id = bot_user_id, for_whom_sex = user_info['sex'], for_whom_byear = user_info['birth_year'], for_whom_city = user_info['city'])
                                vkinder.offer_candidates(for_whom_id=bot_user_id,  candidates_list=any_3_candidates)
                            else:
                                break

if __name__ == "__main__":
    main()