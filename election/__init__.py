import json
import logging
import os
from concurrent import futures
from datetime import datetime

import election_service.election_grpc_pb2 as election_pb2
import election_service.election_grpc_pb2_grpc as election_pb2_grpc
import grpc
import pika
from dotenv import load_dotenv
from orm_models import Candidate, Capybara, Election, User, Vote
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker

load_dotenv()

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - '
                           '%(levelname)s - %(message)s')

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
        capybara = session.query(Capybara).filter(Capybara.login == f"{username}@student.21-school.ru", Capybara.is_student == True).first()
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

    def GetCandidates(self, request, context):
        avatars = [
            "https://s0.rbk.ru/v6_top_pics/media/img/5/90/756781807819905.webp",
            "https://www.newsler.ru/data/content/2022/124529/fa587f34b2eef1d2847300e8472d702f.RPPkxCdwIDkMKjtWDnCVwqu5LZH6AC7uLkE_vf0uMbo",
            "https://cdn.iportal.ru/news/2015/99/preview/373040953a4a814a9f983988a24f4a2b06e70980_1280_853_c.jpg",
            "https://static.tildacdn.com/tild3762-3361-4530-b338-636461613366/1663115153_50-mykale.jpg",
            "https://media.tinkoff.ru/stories/media/images/744dcd9447deeb0a8f10e024eb8616e5.png",
            "https://cdn-st1.rtr-vesti.ru/vh/pictures/hd/413/562/0.jpg",
            "https://natworld.info/wp-content/uploads/2018/02/vodosvinka-ili-kapibara.jpg"
        ]
        session = Session()
        now = datetime.now()
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        if not election:
            session.close()
            return election_pb2.GetCandidatesResponse(status=1, description="Not Found Election")

        candidates = session.query(Candidate).filter(Candidate.election_id == election.id,
                                                     Candidate.is_approved == True).all()
        result = []
        i = 0
        for candidate in candidates:
            login = session.query(Capybara).filter(Capybara.id == candidate.user_id).first()
            result.append(election_pb2.Candidate(avatar=avatars[i],
                                                 login=login.login.split("@")[0],
                                                 id=candidate.id,
                                                 about=candidate.about))
            i += 1
        session.close()
        return election_pb2.GetCandidatesResponse(status=0, description="Success", candidates=result)

    def MyCandidatesTmp(self, request, context):
        uuid = request.uuid
        session = Session()
        now = datetime.now()
        user = session.query(Capybara).filter(Capybara.school_user_id == uuid, Capybara.is_student == True).first()
        if not user:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Пользователь не найден")
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        if not election:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Выборы не найдены")
        candidates = session.query(Vote).filter(Vote.election_id == election.id,
                                                Vote.voter_id == user.id).all()
        result = []
        for candidate in candidates:
            candidate_ = session.query(Candidate).filter(Candidate.id == candidate.candidate_id).first()
            login = session.query(Capybara).filter(Capybara.id == candidate_.user_id).first()
            result.append(election_pb2.Candidate(login=login.login.split("@")[0],
                                                 id=candidate.candidate_id))
        session.close()
        return election_pb2.MyCandidateResponse(status=0, description="Success", count=len(result), candidates=result)

    def MyCandidatesCapy(self, request, context):
        uuid = request.uuid
        session = Session()
        now = datetime.now()
        school_uuid = session.query(User).filter(User.capy_uuid == uuid).first()
        user = session.query(Capybara).filter(Capybara.school_user_id == school_uuid.school_user_id,
                                              Capybara.is_student == True).first()
        if not user:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Пользователь не найден")
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        if not election:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Выборы не найдены")
        candidates = session.query(Vote).filter(Vote.election_id == election.id,
                                                Vote.voter_id == user.id).all()
        result = []
        for candidate in candidates:
            candidate_ = session.query(Candidate).filter(Candidate.id == candidate.candidate_id).first()
            login = session.query(Capybara).filter(Capybara.id == candidate_.user_id).first()
            result.append(election_pb2.Candidate(login=login.login.split("@")[0],
                                                 id=candidate.candidate_id))
        session.close()
        return election_pb2.MyCandidateResponse(status=0, description="Success", count=len(result), candidates=result)

    def VoteTmp(self, request, context):
        uuid = request.uuid
        id = request.candidate_id
        session = Session()
        now = datetime.now()
        user = session.query(Capybara).filter(Capybara.school_user_id == uuid, Capybara.is_student == True).first()
        if not user:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Пользователь не найден")
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        if not election:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Выборы не найдены")

        candidate = session.query(Candidate).filter(Candidate.id == id).first()
        if not candidate:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Кандидат не найден")

        check_exist = session.query(Vote).filter(Vote.election_id == election.id,
                                                 Vote.voter_id == user.id,
                                                 Vote.candidate_id == candidate.id).first()

        if check_exist:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Голос за этого кандидата уже отдан")

        vote_record = Vote(election_id=election.id,
                           voter_id=user.id,
                           candidate_id=candidate.id)
        session.add(vote_record)
        session.commit()
        session.close()
        return election_pb2.VoteResponse(status=0, description="Success")

    def VoteCapy(self, request, context):
        uuid = request.uuid
        id = request.candidate_id
        session = Session()
        now = datetime.now()
        local_user = session.query(User).filter(User.capy_uuid == uuid).first()
        print(local_user.id)
        if not local_user:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Пользователь не найден")
        user = session.query(Capybara).filter(Capybara.school_user_id == local_user.school_user_id,
                                              Capybara.is_student == True).first()
        if not user:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Пользователь не найден")
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        if not election:
            session.close()
            return election_pb2.MyCandidateResponse(status=1, description="Выборы не найдены")

        candidate = session.query(Candidate).filter(Candidate.id == id).first()
        if not candidate:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Кандидат не найден")

        check_exist = session.query(Vote).filter(Vote.election_id == election.id,
                                                 Vote.voter_id == user.id,
                                                 Vote.candidate_id == candidate.id).first()

        if check_exist:
            session.close()
            return election_pb2.VoteResponse(status=1, description="Голос за этого кандидата уже отдан")

        vote_record = Vote(election_id=election.id,
                           voter_id=user.id,
                           candidate_id=candidate.id)
        session.add(vote_record)
        session.commit()
        session.close()
        return election_pb2.VoteResponse(status=0, description="Success")

    def GetStatistic(self, request, context):
        session = Session()
        now = datetime.now()
        election = session.query(Election).filter(Election.time_finish_collection < now,
                                                  Election.is_finished == False).first()
        candidates = session.query(Candidate).filter(Candidate.is_approved == True,
                                                     Candidate.election_id == election.id).all()
        result = []
        all_votes = 0
        for candidate in candidates:
            login = session.query(Capybara).filter(Capybara.id == candidate.user_id).first()
            votes = session.query(Vote).filter(Vote.candidate_id == candidate.id).all()
            print(login.login.split("@")[0], len(votes))
            all_votes += len(votes)
            result.append({
                "nickname": login.login.split("@")[0],
                "count": len(votes),
                "percent": 0.0
            })

        for i in result:
            try:
                i["percent"] = (i["count"] / all_votes) * 100
            except:
                i["percent"] = 0

        result = sorted(result, key=lambda x: x["count"], reverse=True)

        all_capybaras = session.query(Capybara).filter(Capybara.is_student == True).all()
        unique_voters_count = session.query(func.count(Vote.voter_id.distinct())).scalar()
        session.close()
        return election_pb2.GetStatisticResponse(
            candidates=[election_pb2.CandidateStat(
                nickname=i["nickname"],
                count=i["count"],
                percent=i["percent"]) for i in result],
            all_capybaras=len(all_capybaras),
            count_voter=unique_voters_count,
            percent_voter=(unique_voters_count / len(all_capybaras)) * 100
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    election_pb2_grpc.add_ElectionServiceServicer_to_server(ElectionService(), server)
    server.add_insecure_port(f'[::]:{os.getenv("GRPC_PORT")}')
    logging.info(f"start on {os.getenv('GRPC_PORT')}")
    print("start on", os.getenv("GRPC_PORT"))
    server.start()
    server.wait_for_termination()
