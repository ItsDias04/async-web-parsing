from bs4 import BeautifulSoup
import asyncio
import aiohttp
import pandas as pd
from random import choice
from time import sleep
import json
import requests
import os
import random as rd
import aiofile


data = []
products_urls = {}
images = {}
categories = {}


def read_json(name):
    with open(name, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


headers = read_json('headerd_v2.json')


def save_json(name, data):
    with open(name, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


async def get_html(url, client: aiohttp):
    while True:
        try:
            _headers_ = choice(headers)
            response = await asyncio.create_task(client.get(url, headers=_headers_))
        except Exception as er:
            print(url)
            print(er)
            sleep(0.1)
        else:
            res = await asyncio.create_task(response.text())
            break
    return res


def get_page(link):
    while True:
        try:
            _headers_ = choice(headers)
            html = requests.get(link, headers=_headers_).text
        except:
            pass
        else:
            return html


async def async_get_image(url, client: aiohttp.ClientSession):
    while True:
        try:
            response = await client.get(url, headers=choice(headers))
            
            image = await response.read()
        except Exception as ex:
            pass
        else:
            break
    return image


async def save_image(img, path, name):
    async with aiofile.async_open(f'{path}/{name}', 'wb') as file:
        await asyncio.create_task(file.write(img))


async def get_image(name, url, client):
    image = await asyncio.create_task(async_get_image(url, client))
    try:
        os.mkdir("images")
    except FileExistsError:
        pass
    await asyncio.create_task(save_image(image, 'images', name))
    


async def get_images():
    tasks = []
    client = aiohttp.ClientSession(trust_env=True)

    for image_name in images:
        if image_name is '':
            continue
        tasks.append(asyncio.create_task(get_image(image_name, images[image_name], client)))

    await asyncio.gather(*tasks)
    await client.close()



def start_async_function(func, *atributes_tuple, **atributes_dict):
    loop = asyncio.get_event_loop()
    if atributes_tuple is () and atributes_dict is {}:
        loop.run_until_complete(func())
    elif atributes_tuple is not () and atributes_dict is {}:
        loop.run_until_complete(func(*atributes_tuple))
    elif atributes_tuple is () and atributes_dict is not {}:
        loop.run_until_complete(func(**atributes_dict))
    elif atributes_tuple is not () and atributes_dict is not {}:
        loop.run_until_complete(func(*atributes_tuple, **atributes_dict))
    loop.close()


def get_categories():

    html = get_page('https://www.moscow-garden24.ru/')
    soup = BeautifulSoup(html, 'lxml')

    lvl_1 = soup.find('ul', class_='lev1')
    li_lvl1_tags = lvl_1.find_all('li', recursive=False)

    for li_lvl1_tag in li_lvl1_tags[:-3]:

        a = li_lvl1_tag.find('a')
        url_1 = a.get('href').split('/')[-1]
        name_lvl1 = a.text.strip()

        lvl_2 = li_lvl1_tag.find('ul', class_='lev2')
        li_lvl2_tags = lvl_2.find_all('li', recursive=False)

        for li_lvl2_tag in li_lvl2_tags:

            a = li_lvl2_tag.find('a')
            url_category = a.get('href')
            url_2 = url_category.split('/')[-1]
            name_lvl2 = a.text.strip()

            lvl_3 = li_lvl2_tag.find('ul', class_='lev3')
            if lvl_3 is None:
                categories[url_category] = [
                    {
                        'name': name_lvl1,
                        'category_url': url_1
                    },
                    {
                        'name': name_lvl2,
                        'category_url': url_2
                    }
                ]
            else:
                li_lvl3_tags = lvl_3.find_all('li', recursive=False)
                for li_lvl3_tag in li_lvl3_tags:

                    a = li_lvl3_tag.find('a')
                    url_category = a.get('href')
                    url_3 = url_category.split('/')[-1]
                    name_lvl3 = a.text.strip()

                    categories[url_category] = [
                    {
                        'name': name_lvl1,
                        'category_url': url_1
                    },
                    {
                        'name': name_lvl2,
                        'category_url': url_2
                    },
                    {
                        'name': name_lvl3,
                        'category_url': url_3
                    }
                ]


async def get_products_urls_1(url_category, category, client):
    html = await get_html(f"https://www.moscow-garden24.ru/{url_category}/page-all", client)
    soup = BeautifulSoup(html, 'lxml')

    a_tags = soup.find_all('a', class_="product_name")

    for a_tag in a_tags:
        products_urls[a_tag.get('href')] = category



async def get_products_urls_0():

    client = aiohttp.ClientSession(trust_env=True)
    tasks = []

    for url_category in categories:

        tasks.append(asyncio.create_task(get_products_urls_1(url_category, categories[url_category], client)))
    
    await asyncio.gather(*tasks)
    await client.close()


async def get_products_1(url, category, client):
    html = await asyncio.create_task(get_html(f"https://www.moscow-garden24.ru/{url}", client))
    soup = BeautifulSoup(html, 'lxml')

    url_product = url.split('/')[-1]
    name = soup.find('h1', itemprop="name").text.strip()

    img = soup.find('img', itemprop="image")

    img_url = img.get('src')
    img_name = img_url.split('/')[-1]

    images[img_name] = img_url

    product_labels_divs = soup.find('div', class_="product-labels").find_all('div')
    tags = [tag.text.strip() for tag in product_labels_divs]

    rating = float(soup.find('span', itemprop="ratingValue").text.strip()[1:-1])

    description = soup.find('div', class_="col-12 col-lg-11 col-xl-7 pr-xl-2 pl-xl-0")

    if description is None:
        description = ''
    else:
        description = ''.join([str(i) for i in description.contents])

    div_c = soup.find_all('div', class_="features_inline")
    characteristics = []

    for tag_div_c in div_c:
        name_charac = tag_div_c.find('div', class_='name').text.strip()
        value_charac = tag_div_c.find('div', class_='value').text.strip()

        characteristics.append(
            {
                'name': name_charac,
                'value': value_charac
            }
        )
    

    tr_tags = soup.find_all('tr', class_="variant")

    price = []

    for tr_tag in tr_tags:
        age = tr_tag.find_all('td')[1]
        age = age.text.strip()
        _price = tr_tag.find('span', class_="price").text.strip().split(' ')

        price.append(
            {
                'age': age,
                'price': int(''.join(_price[:-1]))
            }
        )

    reviews = []

    comments_divs = soup.find_all('div', class_="comment_content")

    for comment_div in comments_divs:
        reviews.append(comment_div.text.strip())
    
    data.append(
        {
            'url': url_product,
            'name': name,
            'image': img_name,
            'tags': tags,
            'rating': rating,
            'description': description,
            'category': category,
            'characteristics': characteristics,
            'price': price,
            'reviews': reviews
        }
    )


async def get_products_0():

    client = aiohttp.ClientSession(trust_env=True)
    tasks = []

    for products_url in list(products_urls.keys()):

        tasks.append(asyncio.create_task(get_products_1(products_url, products_urls[products_url], client)))
    
    await asyncio.gather(*tasks)    
    await client.close()

    save_json('moscow_garden24.json', data)
    save_json('moscow_garden24_test.json', data[:200])


async def parsing():

    await get_products_urls_0()
    await get_products_0()
    await get_images()


def main():
    get_categories()
    start_async_function(parsing)


if __name__ == "__main__":
    main()