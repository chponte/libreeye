# Messages are delimited by the character \0

def write_msg(socket, message):
    socket.sendall(f'{message}\0'.encode('ascii'))


def read_msg(socket) -> str:
    answer = b''
    while len(answer) == 0 or answer[-1] != ord('\0'):
        answer += socket.recv(1024)
    return answer.decode('ascii')[:-1]
