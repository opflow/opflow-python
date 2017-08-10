#!/usr/bin/env python

import random
import time

class FibonacciGenerator:
	def __init__(self, number, delayMin=None, delayMax=None):
		self.n = number
		self.c = 0
		self.f = self.f_1 = self.f_2 = 0
		self.min = delayMin
		self.max = delayMax

	def next(self):
		if (self.min is not None and self.max is not None and 
				0 <= self.min and self.min < self.max):
			delay = random.randint(self.max - self.min + 1) + self.min
			time.sleep(delay)
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
