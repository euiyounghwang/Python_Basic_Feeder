
import base64

class Base64:

    # def __init__(self):
        # print('\nBase64')

    def Sample(self):
        """

        :return:
        """
        X = '황의영'
        Y = self.stringToBase64(X)
        print('\n')
        print(Y)
        print('\n')
        Z = self.base64ToString(Y)
        print(Z)

    def stringToBase64(self, a):
        return base64.b64encode(a.encode('utf-8'))

    def base64ToString(self, b):
        return base64.b64decode(b).decode()

    def fileToBase64(self, filepath):
        fp = open(filepath, "rb")

        data = fp.read()

        fp.close()

        return base64.b64encode(data).decode('utf-8')


if __name__ == '__main__':

    Base64().Sample()