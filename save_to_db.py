from models import Story, Comment, User, Job, Poll, PollOpt


class SaveToDB:
    @staticmethod
    def create_tables():
        return [
            model().create_table()
            for model in [Story, Comment, User, Job, Poll, PollOpt]
        ]

    @staticmethod
    async def save_user(*, data: dict):
        data = User(**data)
        return await data.insert()

    @staticmethod
    async def save_data(*, data: dict):
        match data.get("type"):
            case "story":
                data = Story(**data)
            case "comment":
                data = Comment(**data)
            case "user":
                data = User(**data)
            case "job":
                data = Job(**data)
            case "poll":
                data = Poll(**data)
            case "pollopt":
                data = PollOpt(**data)
            case _:
                data = None
        if not data:
            return
        return await data.insert()

    @staticmethod
    async def show():
        for model in [Story, Comment, User, Job, Poll, PollOpt]:
            await model().show()
