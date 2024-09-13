from config.luigi import luigi
from datetime import datetime


class job_class_name(luigi.Task):
    def run(self):
        formatted_time = datetime.now().strftime('%b %d, %Y %I:%M %p')  # Format like "Jan 20, 2024 12:30 PM"

        message = f'Date and time: {formatted_time}'

        print('-----')
        print(message)
        print('-----')

        # Mark task as completed
        self.task_completed = True

    def complete(self):
        return getattr(self, 'task_completed', False)


def main():
    luigi.build([job_class_name()], local_scheduler=True)


if __name__ == '__main__':
    main()
