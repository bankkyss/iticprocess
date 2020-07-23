from functools import partial
import multiprocessing 
def f(a, b, c):
    return str(a)+str(b)+str(c)

def main():
    iterable = [1, 2, 3, 4, 5]
    pool = multiprocessing.Pool()
    a = "hi"
    b = "there"
    func = partial(f, a, b)
    outputs_async =pool.map(func, iterable)
    outputs =outputs_async
    print(outputs)
    pool.close()
    pool.join()

if __name__ == "__main__":
    main()