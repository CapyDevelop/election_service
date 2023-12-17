import json
import os
from concurrent import futures
from datetime import datetime

from dotenv import load_dotenv
import election_service.election_grpc_pb2 as election_pb2
import election_service.election_grpc_pb2_grpc as election_pb2_grpc
import grpc
import pika
from orm_models import Election, Candidate, Capybara, User
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


load_dotenv()

engine = create_engine(os.getenv("DB_ENGINE"))
Session = sessionmaker(bind=engine)

print(os.getenv("RABBITMQ_USERNAME"), os.getenv("RABBITMQ_PASSWORD"))
credentials = pika.PlainCredentials(username=os.getenv("RABBITMQ_USERNAME"), password=os.getenv("RABBITMQ_PASSWORD"))





class ElectionService(election_pb2_grpc.ElectionServiceServicer):
    def GetElection(self, request, context):
        session = Session()
        now = datetime.now()
        election = session.query(Election).filter(Election.time_start_collection < now, Election.is_finished == False).first()
        session.close()
        if not election:
            return election_pb2.GetElectionResponse(status=1)
        if election.time_finish_collection > now:
            return election_pb2.GetElectionResponse(status=100)
        if election.time_finish_collection < now < election.time_start_voting:
            return election_pb2.GetElectionResponse(status=101)
        if election.time_start_voting < now < election.time_finish_voting:
            return election_pb2.GetElectionResponse(status=102)
        return election_pb2.GetElectionResponse(status=500)

    def SetCandidateTmp(self, request, context):
        uuid = request.uuid
        about = request.about
        session = Session()
        capybara = session.query(Capybara).filter(Capybara.school_user_id == uuid).first()
        if not capybara:
            session.close()
            return election_pb2.SetCandidateResponse(status=1, description="User Not Found")
        now = datetime.now()
        # FIXME тут трайб - хардкодом. Для масштабирования нужно искать по БД
        election = session.query(Election).filter(
            Election.tribe_id == 1,
            Election.is_finished == False,
            Election.time_start_collection < now,
            Election.time_finish_collection > now
        ).first()
        if not election:
            session.close()
            return election_pb2.SetCandidateResponse(status=2, description="Election Not Found")
        candidate = Candidate(
            election_id=election.id,
            user_id=capybara.id,
            about=about
        )
        session.add(candidate)
        session.commit()
        session.close()

        return election_pb2.SetCandidateResponse(status=0, description="Success")

    def SetCandidateCapy(self, request, context):
        uuid = request.uuid
        about = request.about
        session = Session()
        user = session.query(User).filter(User.capy_uuid == uuid).first()
        if not user:
            session.close()
            return election_pb2.SetCandidateResponse(status=1, description="User Not Found")
        capybara = session.query(Capybara).filter(Capybara.school_user_id == user.school_user_id).first()
        if not capybara:
            session.close()
            return election_pb2.SetCandidateResponse(status=1, description="Capybara Not Found")
        now = datetime.now()
        # FIXME тут трайб - хардкодом. Для масштабирования нужно искать по БД
        election = session.query(Election).filter(
            Election.tribe_id == 1,
            Election.is_finished == False,
            Election.time_start_collection < now,
            Election.time_finish_collection > now
        ).first()
        if not election:
            session.close()
            return election_pb2.SetCandidateResponse(status=2, description="Election Not Found")
        candidate = Candidate(
            election_id=election.id,
            user_id=capybara.id,
            about=about
        )
        session.add(candidate)
        session.commit()
        session.close()

        return election_pb2.SetCandidateResponse(status=0, description="Success")

    def CheckCandidateTmp(self, request, context):
        uuid = request.uuid
        session = Session()
        capybara = session.query(Capybara).filter(Capybara.school_user_id == uuid).first()
        if not capybara:
            session.close()
            return election_pb2.CheckCandidateResponse(status=4, description="Capybara Not Found")
        candidate = session.query(Candidate).filter(Candidate.user_id == capybara.id).first()
        if not candidate:
            session.close()
            return election_pb2.CheckCandidateResponse(status=0, description="User is not candidate")
        if candidate.is_approved:
            session.close()
            return election_pb2.CheckCandidateResponse(status=2, description="User Approved")
        session.close()
        return election_pb2.CheckCandidateResponse(status=1, description="User is Candidate, but not approved")

    def CheckCandidateCapy(self, request, context):
        uuid = request.uuid
        session = Session()
        user = session.query(User).filter(User.capy_uuid == uuid).first()
        if not user:
            session.close()
            return election_pb2.CheckCandidateResponse(status=4, description="User Not Found")
        capybara = session.query(Capybara).filter(Capybara.school_user_id == user.school_user_id).first()
        if not capybara:
            session.close()
            return election_pb2.CheckCandidateResponse(status=4, description="Capybara Not Found")
        candidate = session.query(Candidate).filter(Candidate.user_id == capybara.id).first()
        if not candidate:
            session.close()
            return election_pb2.CheckCandidateResponse(status=0, description="User is not candidate")
        if candidate.is_approved:
            session.close()
            return election_pb2.CheckCandidateResponse(status=2, description="User Approved")
        session.close()
        return election_pb2.CheckCandidateResponse(status=1, description="User is Candidate, but not approved")

    def SendPassword(self, request, context):
        username: str = request.mail
        username = username.lower()
        session = Session()
        capybara = session.query(Capybara).filter(Capybara.login == f"{username}@student.21-school.ru").first()
        session.close()
        if not capybara:
            return election_pb2.SendPasswordResponse(status=1, description="not found capybara")
        message_body = {
            "user": capybara.login,
            "code": capybara.key
        }
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(os.getenv("RABBITMQ_HOST"), port=os.getenv("RABBITMQ_PORT"),
                                      credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue='email_queue')
        message_body = json.dumps(message_body)
        channel.basic_publish(exchange='', routing_key='email_queue', body=message_body)
        channel.close()
        connection.close()
        return election_pb2.SendPasswordResponse(status=0, description="Success send in queue")

    def ConfirmPassword(self, request, context):
        username = request.mail
        username = username.lower()
        password = request.password
        session = Session()
        capybara = session.query(Capybara).filter(Capybara.login == f"{username}@student.21-school.ru").first()
        session.close()
        if not capybara:
            return election_pb2.ConfirmPasswordResponse(status=2, description="Не найден логин в капибарах")
        if password != capybara.key:
            return election_pb2.ConfirmPasswordResponse(status=1, description="Пароли не совпадают")
        return election_pb2.ConfirmPasswordResponse(status=0, description="success", uuid=capybara.school_user_id)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    election_pb2_grpc.add_ElectionServiceServicer_to_server(ElectionService(), server)
    server.add_insecure_port(f'[::]:{os.getenv("GRPC_PORT")}')
    print("start on", os.getenv("GRPC_PORT"))
    server.start()
    server.wait_for_termination()
