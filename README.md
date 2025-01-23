# ejudge-ws-server-go

Эта программа реализует сервер нотификаций ejudge. Программа обрабатывает подключения
по http, аутентифицирует пользователя по хедеру `Authorization`, и переключает соединение
в режим websocket. Далее все нотификации, которые ejudge генерирует в redis stream
будут отправляться подключившемуся клиенту. То есть данная программа выступает в роли
прокси между вебсокетами и redis stream.

## Подключение к сервису

Подключение к сервису выполняется по URL `/ws/:STREAM/:SERIAL`. Здесь `STREAM` &mdash; это
имя redis stream, в который ejudge отправляет нотификации. `SERIAL` &mdash; это
номер сообщения, с которого должна начаться выдача.

## Переменные окружения

* `MYSQL_HOST` &mdash; адрес хоста с сервером с MariaDB с базой ejudge. По умолчанию `localhost`.
* `MYSQL_PORT` &mdash; адрес порта MariaDB. По умолчанию `3306`.
* `MYSQL_DATABASE` &mdash; имя базы данных ejudge. По умолчанию `ejudge`.
* `MYSQL_USER` &mdash; имя пользователя. По умолчанию `ejudge`.
* `MYSQL_PASSWORD` &mdash; пароль от MariaDB. Должен быть специфицирован.
* `REDIS_HOST` &mdash; адрес хоста redis. По умолчанию `localhost`.
* `REDIS_PORT` &mdash; номер порта redis. По умолчанию `6379`.
* `REDIS_PASSWORD` &mdash; пароль от redis. По умолчанию не установлен.
* `GIN_PORT` &mdash; номер порта, на котором сервис будет ожидать подключение

## Использование в ejudge

В конфигурационный файл `ejudge.xml` необходимо добавить конфигурацию плагина `redis_streams`:

```
    <plugin type="notify" name="redis_streams" load="yes">
      <config />
    </plugin>
```

Если доступ к redis закрыт паролем, нужно сохранить пароль в файле и добавить его имя в конфигурацию.

```
    <plugin type="notify" name="redis_streams" load="yes">
      <config>
        <password_file>redis_passwd</password_file>
      </config>
    </plugin>
```

Чтобы в redis отправлялись нотификации по всем посылкам в турнир, в конфигурационный файл турнира `serve.cfg` нужно
добавить глобальный конфигурационный параметр:

```
notification_spec = "2:str:StreamName"
```

здесь `2` &mdash; код плагина `redis_streams`, `str` &mdash; тип идентификатора, `StreamName` &mdash; идентификатор очереди redis.
Длина идентификатора очереди не может превышать 15 символов.

Либо при отправке посылки на проверку с помощью API `submit-run` можно указать три дополнительных параметра запроса:
`notify_driver=2`, `notify_kind=str`, `notify_queue=StreamName`

