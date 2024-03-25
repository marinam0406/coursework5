from config import config
from src.db_manager import DBManager
from src.utils import get_hh_data, create_database, save_data_to_database


def main():
    params = config()
    # список ID компаний: "Яндекс", "VK", "OZON", "2GIS", "Контур",
    # "Kaspersky", "ЦИАН", "Битрикс24", "NAUMEN", "Skyeng"
    employers = ['1740', '15478', '2180', '64174', '41862',
                 '1057', '1429999', '129044', '42600', '1122462']
    data = get_hh_data(employers)
    create_database('headhunter', params)
    save_data_to_database(data, 'headhunter', params)
    db_manager = DBManager('headhunter', params)
    print('Вам будет представлена информация о вакансиях следующих компаний:\n'
          '"Яндекс", "VK", "OZON", "2GIS", "Контур", "Kaspersky", "ЦИАН", \n'
          '"Битрикс24", "NAUMEN", "Skyeng"\n')
    input("Для продолжения нажмите Enter ")

    print("Список компаний и количество вакансий в компаниях:")
    for row in db_manager.get_companies_and_vacancies_count():
        print(f"{row[0]} - {row[1]}")

    input("\nДля продолжения нажмите Enter ")
    print("Список всех вакансий с указанием названия компании,"
          "названия вакансии и зарплаты и ссылки на вакансию:")
    for row in db_manager.get_all_vacancies():
        print(f"{row[0]} - {row[1]} - Минимальная заработная плата: {row[2]} - Ссылка: {row[3]}")

    input("\nДля продолжения нажмите Enter ")
    print("Cредняя зарплата по вакансиям:")
    for row in db_manager.get_avg_salary():
        print(f"{row[0]} - {row[1]}")

    input("\nДля продолжения нажмите Enter ")
    print("Cписок всех вакансий, у которых зарплата выше средней по всем вакансиям:")
    for row in db_manager.get_vacancies_with_higher_salary():
        print(f"{row[1]} - {row[3]} - {row[7]} - {row[8]}")

    input("\nДля продолжения нажмите Enter ")
    user_input = input("Чтобы получить список всех вакансий, "
                       "в названии которых содержатся переданные в метод слова, "
                       "введите например 'менеджер': ")
    for row in db_manager.get_vacancies_with_keyword(user_input):
        print(f"{row[1]} - {row[3]} - {row[7]} - {row[8]}")


if __name__ == '__main__':
    main()