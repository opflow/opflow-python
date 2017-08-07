#!/usr/bin/env python

class FibonacciGenerator:
	def __init__(self, number):
		self.n = number
		self.c = 0
		self.f = self.f_1 = self.f_2 = 0

	def next(self):
		if (self.c >= self.n): return False
		self.c = self.c + 1
		if (self.c < 2):
			self.f = self.c
		else:
			self.f_2 = self.f_1
			self.f_1 = self.f
			self.f = self.f_1 + self.f_2
		return True

	def result(self):
		return { 'value': self.f, 'step': self.c, 'number': self.n }

	def finish(self):
		while(self.next()): True
		return self.result()

# fg = FibonacciGenerator(20)
# result = fg.finish()
# print result
