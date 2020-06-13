package edu.carleton.enchilada.ATOFMS;

public class ATOFMSPeak extends Peak {
	public int height;
	public int area;
	public float relArea;
	public double massToCharge;
	
	public ATOFMSPeak(double v, double mz) {
		super(v, mz);
		// TODO Auto-generated constructor stub
		height = -1;
		area = -1;
		relArea = -1;
		massToCharge = -1;
	}

	public ATOFMSPeak (int h, int a, float relA, double mz)
	{
		this(a,mz);
		height = h;
		area = a;
		relArea = relA;
		massToCharge = mz;
	}
	
	public ATOFMSPeak (int h, int a, double mz)
	{
		this(a,mz);
		height = h;
		area = a;
		relArea = 0;
		massToCharge = mz;
	}
	
	/**
	 * prints peak.
	 */
	public String toString()
	{
		String returnThis =
			"Location: " + massToCharge + 
			" Height: " + height +
			" Area: " + area + 
			" Rel. Area: " + relArea;
		return returnThis;
	}

	public String toCommaDelimitedString() {
		return massToCharge +
				", " + area +
				", " + relArea +
				", " + height;
	}
}
