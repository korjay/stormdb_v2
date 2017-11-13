package Air;

import java.io.Serializable;

/**
 * Created by honey on 17. 11. 6.
 */
public class Obj_so implements Serializable {
    private static Object[] ObjArr = null;
    private static int flag=0;
    public int num = 0;
//    public int num1 = 0;

    public void setValue(Object[] obj){
        ObjArr=obj;
    }

    public void setFlag(int i)
    {
        flag = i;
    }

    public Object[] getValue(){
        return ObjArr;
    }

    public void setNum(int i) {num = i;}

    public int getNum(){
        return num;
    }

//    public void setNum1(int j){num1 = j;}
//
//    public int getNum1(){return num1;}



    public int getFlag()
    {
        return flag;
    }
}
