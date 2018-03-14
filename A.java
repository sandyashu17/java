class A
{
int a,b,c;
public void add()
{
a=5;
b=6;
c=a+b;
}
public void display()
{
System.out.println(c);
}
public static void main(String args[])
{
A ss=new A();
ss.add();
ss.display();                 
\\ss.add();
}   
}